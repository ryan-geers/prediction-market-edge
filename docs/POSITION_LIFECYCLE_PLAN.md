# Position Lifecycle — Implementation Plan

**Purpose:** Define a phased roadmap to evolve paper trading from "open a new row every run" into a system with real position management — re-marking, exits, deduplication, and scale-in/out — so we can actually evaluate whether the thesis is generating positive expected value.

---

## Background: What exists today

| Component | Status |
|-----------|--------|
| Signal generation (edge vs threshold) | ✅ Working |
| Paper order + position row on entry signal | ✅ Working |
| Bankroll-based position sizing | ✅ Done (pre-Phase 1) |
| `status='open'` rows never updated after first run | ❌ Missing |
| Mark-to-market on subsequent runs | ❌ Missing |
| Exit logic (any kind) | ❌ Missing |
| Deduplication (already in position check) | ❌ Missing |
| Scale-in / scale-out | ❌ Missing |

The `paper_positions` schema already has the columns needed (`status`, `closed_at_utc`, `avg_exit_price`, `realized_pnl`, `mark_price`, `close_reason`). No schema migrations are required for Phase 1–2. Phase 3 adds one column.

---

## Pre-Phase 1 — Bankroll-based position sizing ✅ Done

**Problem:** `paper_default_qty=1.0` meant every entry bought exactly 1 contract (~$0.50 notional). P&L numbers were meaningless fractions of a cent.

**Change:** Two new settings in `config.py`:

```python
paper_bankroll: float = 500.0          # total simulated dollars
paper_position_size_pct: float = 0.05  # 5% of bankroll per position = $25
```

`paper_trading.py` derives contract qty at entry time:

```python
qty = (paper_bankroll * paper_position_size_pct) / fill_price
# e.g. ($500 × 5%) / $0.50 = 50 contracts per entry
```

Dollar exposure per position is now consistent (~$25); contract count varies with price. Override via `.env` or GitHub Actions variables if you want a different account size or risk per trade.

---



**Goal:** Stop showing stale `unrealized_pnl` and `mark_price` in the digest. Every run that touches a contract should update existing open rows for that contract with the current market mid.

### What changes

#### `src/core/storage.py`
Add `Storage.mark_open_positions(marks: list[PositionMark])` where `PositionMark` is a small dataclass/dict of `{contract_id, venue, mark_price, unrealized_pnl, last_mark_time_utc}`.

```sql
UPDATE paper_positions
SET
  mark_price = ?,
  unrealized_pnl = (mark_price - avg_entry_price) * net_qty,
  last_mark_time_utc = ?
WHERE status = 'open'
  AND contract_id = ?
  AND venue = ?
```

#### `src/pipeline/run.py`
After `thesis.generate_signals(...)` and before writing new positions, call `storage.mark_open_positions(...)` using the current market snapshots already in memory.

#### `src/pipeline/paper_trading.py`
No change needed in Phase 1 — open positions are still created as today; they just get updated on subsequent runs.

### Tests
- Unit: given open position row and new mark price, verify `unrealized_pnl` and `mark_price` are updated in DB.
- Integration: run pipeline twice; confirm `mark_price` on the first-run row has been updated after the second run.

### Observable outcome
Digest TL;DR shows a **live** unrealized PnL rather than entry-day stale figures. Weekly bank numbers become meaningful.

---

## Phase 2 — Exit logic (close positions)

**Goal:** Positions actually close when conditions warrant. Realized PnL becomes the true scorecard.

### Exit rules (implement in order; each is a knob in `Settings`)

#### Rule A — Edge flip / signal reversal (primary)
Close an open `enter_long_yes` position when the same contract's current signal is `enter_long_no` (or vice versa). The thesis disagrees with the original bet.

```
if existing open position is 'yes' AND current edge_bps < -edge_threshold_bps:
    exit at current market mid
if existing open position is 'no'  AND current edge_bps > +edge_threshold_bps:
    exit at current market mid
```

Setting: `paper_exit_on_flip: bool = True`

#### Rule B — Stop-loss
Close a position when `unrealized_pnl / (avg_entry_price * net_qty)` falls below a threshold (e.g. -15%).

Setting: `paper_stop_loss_pct: float | None = None` (disabled by default until calibrated)

#### Rule C — Contract resolution / expiry proximity
Close positions on contracts that the exchange marks as `settled` or `closed`, or where the expiry timestamp is within N hours.

Setting: `paper_close_on_settle: bool = True`

### What changes

#### `src/core/schemas.py`
`close_reason` is already `str | None`. Use string values: `"signal_flip"`, `"stop_loss"`, `"contract_settled"`, `"manual"`.

#### `src/pipeline/paper_trading.py`
Add `apply_exits(open_positions, signals, snapshots, settings) -> list[PositionClose]` where `PositionClose` captures `{position_id, avg_exit_price, realized_pnl, close_reason, closed_at_utc}`.

#### `src/core/storage.py`
Add `Storage.close_positions(closes: list[PositionClose])`:

```sql
UPDATE paper_positions
SET status='closed', closed_at_utc=?, avg_exit_price=?,
    realized_pnl=?, unrealized_pnl=0.0, close_reason=?
WHERE position_id = ?
```

#### `src/pipeline/run.py`
Insert exit step between signal generation and new position entry:

```
signals, snapshots  ← thesis.generate_signals(...)
closes              ← apply_exits(open_positions, signals, snapshots, settings)
storage.close_positions(closes)
orders, positions   ← thesis.paper_trade(signals)   # only new entries
storage.insert_orders / insert_positions
```

Note: `thesis.paper_trade` still only creates **new** entries; exits are handled by the separate layer above it so the thesis code stays clean.

### Tests
- Edge-flip rule closes YES position when NO signal fires.
- Stop-loss fires at correct threshold and doesn't fire below it.
- Position that is already closed is not closed again.

### Observable outcome
- `realized_pnl` rows in DB become populated during normal operation (not just in `eod_mark` mode).
- Digest winners/losers tables have real data.
- **Bank** in TL;DR reflects genuine P&L from settled paper trades.

---

## Phase 3 — Deduplication (one position per contract)

**Goal:** Each contract has at most one open position row. Entering a signal on a contract you already hold merges into the existing row rather than opening a duplicate.

### Problem today
28 pipeline runs on the same contract with `enter_long_yes` produces 28 open position rows, each with `net_qty=1.0`. This massively overstates exposure and makes P&L meaningless.

### Schema change
Add `paper_positions.direction VARCHAR` (values `'yes'`, `'no'`). Migration:

```sql
ALTER TABLE paper_positions ADD COLUMN IF NOT EXISTS direction VARCHAR;
```

Add to `SCHEMA_MIGRATIONS` in `storage.py`.

Update `PaperPositionRecord` in `schemas.py` and `simulate_paper_trades` in `paper_trading.py` to populate the field.

### Logic

**Before creating a new position:**

```python
existing = storage.get_open_position(contract_id, venue, direction)
if existing:
    storage.add_to_position(existing.position_id, new_qty, new_fill_price)
    # updates avg_entry_price (VWAP), net_qty, logs an order but no new position row
else:
    storage.insert_position(new_position)
```

**Average-in formula (VWAP):**
```
new_avg = (old_avg * old_qty + fill_price * new_qty) / (old_qty + new_qty)
```

Settings for this phase:
- `paper_allow_add_to_position: bool = False` — off by default so existing behavior is preserved until you deliberately enable it.

### Tests
- Two signals on same contract, same direction: one position row with combined qty and VWAP entry.
- Two signals on same contract, opposite directions: both rows exist (existing close logic from Phase 2 handles the flip before Phase 3 would add to it).

### Observable outcome
- Position count in TL;DR "open positions" reflects unique contracts held, not run count.
- Unrealized PnL is meaningful at a portfolio level.

---

## Phase 4 — Scale-in / scale-out (optional, post-validation)

**Implement only after Phases 1–3 have accumulated enough history to evaluate whether the thesis has positive edge.** This phase is deliberate — adding position sizing before you know the thesis works is premature.

### Scale-in
Increase `net_qty` when edge widens beyond a second threshold (e.g. `edge_bps > 2 × edge_threshold_bps`).

Setting: `paper_scale_in_edge_multiplier: float | None = None`

### Scale-out / trim
Reduce `net_qty` proportionally when edge narrows toward zero but hasn't flipped (partial exit).

Setting: `paper_scale_out_edge_pct: float | None = None` (e.g. 0.5 = reduce by 50% when edge halves)

### Requires Phase 3 first
Scale-in/out only makes sense once deduplication is in place and `net_qty` represents a real aggregate position.

---

## Sequencing and dependencies

```
Phase 1 (Re-mark)
    └── Phase 2 (Exits)          ← depends on marks being current
            └── Phase 3 (Dedup) ← depends on exits so flips are handled cleanly
                    └── Phase 4 (Scale) ← depends on dedup so qty is meaningful
```

Each phase is independently deployable and testable without breaking anything above it, because the new logic paths are either additive (Phase 1) or gated by existing conditions (Phases 2–3).

---

## How to know if the thesis is working (success criteria)

After Phases 1–2 are live for at least **4–6 weeks** (enough for several model refresh cycles and at least one Kalshi contract resolution):

| Metric | Where to read it | What "working" looks like |
|--------|-----------------|---------------------------|
| **Lifetime realized sum** | Digest TL;DR Bank | Positive and growing |
| **Hit rate** | Digest cohort section | >50% on closed positions |
| **Avg realized per close** | Digest TL;DR Bank | Positive; outweighs slippage assumption |
| **Edge bps distribution** | Signals section | Mean > 0 over time; not one-directional noise |
| **Model RMSE trend** | Forecast records in DuckDB / artifacts | Declining or stable; large RMSE = model is wrong |
| **Flip rate** | Count `close_reason='signal_flip'` / total opens | High flip rate = model changes opinion too fast = overfit or stale data |

If after 4–6 weeks hit rate is below 45% and avg realized is negative, the thesis (CPI regression → Kalshi contracts) is not generating edge at the current threshold. That would be the signal to revisit the model, the feature set, or the contracts being traded — **not** to add complexity.

---

## Files that will be touched per phase

| File | Ph 1 | Ph 2 | Ph 3 | Ph 4 |
|------|------|------|------|------|
| `src/core/schemas.py` | | | ✏️ add `direction` | ✏️ add sizing fields |
| `src/core/config.py` | | ✏️ exit settings | ✏️ dedup setting | ✏️ scale settings |
| `src/core/storage.py` | ✏️ `mark_open_positions` | ✏️ `close_positions` | ✏️ `get_open_position`, `add_to_position`, migration | |
| `src/pipeline/paper_trading.py` | | ✏️ `apply_exits` | ✏️ dedup logic | ✏️ scale logic |
| `src/pipeline/run.py` | ✏️ call mark | ✏️ call exits | | |
| `src/pipeline/reporting.py` | already queries marks | `close_reason` grouping | position count fix | |
| `tests/test_paper_trading.py` | new | new | new | new |
| `tests/test_storage.py` | new | new | new | |

---

## What does NOT change in any phase

- `ThesisModule` interface — `generate_signals` and `paper_trade` signatures are unchanged.
- DuckDB schema columns in `paper_positions` — all needed fields already exist through Phase 2; Phase 3 adds one column via migration.
- CI workflows — no workflow changes needed; `pme run` gains new behavior transparently.
- Signal generation logic — this plan is purely position lifecycle; edge calculation, model, and connectors are untouched.
