"""
Paper-trade simulation: map signal decisions to filled orders and position rows
with slippage, fee assumptions, and mark-to-market (optional same-run EOD close).
"""
from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

from src.core.schemas import (
    AddToPosition,
    MarketSnapshotRecord,
    PaperOrderRecord,
    PaperPositionRecord,
    PositionClose,
    SignalRecord,
    utc_now,
)

if TYPE_CHECKING:
    from src.core.config import Settings


def _apply_slippage_to_yes_ask(ask: float, slippage_bps: float) -> float:
    adj = ask * (1.0 + slippage_bps / 10000.0)
    return min(0.999, max(0.001, adj))


def _apply_slippage_to_no_ask(yes_bid: float, slippage_bps: float) -> float:
    """Cost to lift NO (1 - yes_bid) with slippage on that price."""
    no_ask = 1.0 - yes_bid
    adj = no_ask * (1.0 + slippage_bps / 10000.0)
    return min(0.999, max(0.001, adj))


def _no_mark_value(yes_mid: float) -> float:
    return 1.0 - yes_mid


def _position_qty(fill_price: float, settings: "Settings") -> float:
    """
    Derive contract quantity from bankroll and position-size percentage.
    budget = bankroll * position_size_pct (e.g. $500 * 5% = $25)
    qty    = budget / fill_price          (e.g. $25 / $0.50 = 50 contracts)
    Falls back to paper_default_qty if fill_price is zero or either setting is unset.
    """
    try:
        budget = float(settings.paper_bankroll) * float(settings.paper_position_size_pct)
        if fill_price > 0 and budget > 0:
            return budget / fill_price
    except (TypeError, ValueError, ZeroDivisionError):
        pass
    return float(settings.paper_default_qty)


def apply_exits(
    open_positions: list[PaperPositionRecord],
    signals: list[SignalRecord],
    snapshots: list[MarketSnapshotRecord],
    settings: Settings,
) -> list[PositionClose]:
    """
    Evaluate exit rules against currently-open positions and return a list of
    positions that should be closed this run.

    Rules (applied in order; first match wins):
      A. Signal flip  — close when the thesis reverses its opinion (paper_exit_on_flip)
      B. Stop-loss    — close when unrealised loss exceeds a % of cost basis (paper_stop_loss_pct)
      C. Settlement   — close when the connector marks a contract settled (paper_close_on_settle)
                        [fires only when snapshot data includes settlement status; stub for now]
    """
    closes: list[PositionClose] = []
    now = utc_now()
    threshold_bps = float(settings.edge_threshold_bps)

    # Build O(1) lookups from the current run's data.
    signal_by_contract: dict[tuple[str, str], SignalRecord] = {
        (s.contract_id, s.venue): s for s in signals
    }
    snap_by_contract: dict[tuple[str, str], MarketSnapshotRecord] = {
        (sn.contract_id, sn.venue): sn for sn in snapshots
    }

    already_closing: set[str] = set()

    for pos in open_positions:
        if pos.status != "open":
            continue

        key = (pos.contract_id, pos.venue)

        # ── Rule A: edge flip / signal reversal ──────────────────────────────
        if settings.paper_exit_on_flip and pos.direction is not None:
            sig = signal_by_contract.get(key)
            if sig is not None:
                flip_yes = pos.direction == "yes" and sig.edge_bps < -threshold_bps
                flip_no = pos.direction == "no" and sig.edge_bps > threshold_bps
                if flip_yes or flip_no:
                    snap = snap_by_contract.get(key)
                    yes_mid = snap.mid_price if snap else float(sig.market_implied_probability)
                    exit_px = yes_mid if pos.direction == "yes" else (1.0 - yes_mid)
                    realized = (exit_px - pos.avg_entry_price) * pos.net_qty
                    closes.append(
                        PositionClose(
                            position_id=pos.position_id,
                            avg_exit_price=exit_px,
                            realized_pnl=realized,
                            close_reason="signal_flip",
                            closed_at_utc=now,
                        )
                    )
                    already_closing.add(pos.position_id)
                    continue

        # ── Rule B: stop-loss ─────────────────────────────────────────────────
        if (
            settings.paper_stop_loss_pct is not None
            and pos.position_id not in already_closing
        ):
            cost_basis = pos.avg_entry_price * pos.net_qty
            if cost_basis > 0:
                loss_pct = pos.unrealized_pnl / cost_basis
                if loss_pct < -abs(settings.paper_stop_loss_pct):
                    snap = snap_by_contract.get(key)
                    yes_mid = (
                        snap.mid_price
                        if snap
                        else (pos.mark_price if pos.mark_price is not None else pos.avg_entry_price)
                    )
                    exit_px = yes_mid if (pos.direction != "no") else (1.0 - yes_mid)
                    realized = (exit_px - pos.avg_entry_price) * pos.net_qty
                    closes.append(
                        PositionClose(
                            position_id=pos.position_id,
                            avg_exit_price=exit_px,
                            realized_pnl=realized,
                            close_reason="stop_loss",
                            closed_at_utc=now,
                        )
                    )
                    already_closing.add(pos.position_id)
                    continue

        # ── Rule C: contract settlement ───────────────────────────────────────
        # Activates when connector-level settlement data is available.
        # Placeholder: no snapshot data currently carries a settled/closed flag,
        # so this rule is wired but dormant until connector support is added.

    return closes


def apply_dedup(
    candidates: list[PaperPositionRecord],
    existing_by_key: dict[tuple[str, str, str], PaperPositionRecord],
    settings: Settings,
    open_counts_by_key: dict[tuple[str, str, str], int] | None = None,
) -> tuple[list[PaperPositionRecord], list[AddToPosition]]:
    """
    Deduplicate new-entry candidates against currently-open positions.

    When ``paper_allow_add_to_position`` is False (default), candidates whose
    (contract_id, venue, direction) key already has an open position are
    **dropped** — the pipeline skips re-entering a contract it already holds.
    Candidates for new contracts (no existing open position) are passed through
    as new inserts unchanged.

    When ``paper_allow_add_to_position`` is True, existing positions are
    VWAP-merged with the new fill instead of dropped.

    ``open_counts_by_key`` provides the total count of open rows per
    (contract_id, venue, direction) key, including rows whose direction is NULL
    (stored under key (contract_id, venue, "")). This is used to enforce
    ``settings.paper_max_open_per_key`` even when old positions have a NULL
    direction and are therefore absent from ``existing_by_key``.

    Closed candidates (eod_close mode) and direction-less rows always pass
    through as new inserts regardless of the flag.

    Returns:
        new_positions      — list of PaperPositionRecord to insert fresh into DB
        add_tos            — list of AddToPosition (VWAP merge ops) to apply to existing rows
        acted_signal_ids   — set of signal_ids for candidates that were opened or merged
                             (i.e. NOT dropped by dedup). Use to filter which orders to persist.
    """
    new_positions: list[PaperPositionRecord] = []
    add_tos: list[AddToPosition] = []
    acted_signal_ids: set[str] = set()
    max_open = int(settings.paper_max_open_per_key)
    max_total = int(settings.paper_max_total_open)

    # Current portfolio size = sum of all open-count values (each key is one row).
    total_currently_open: int = (
        sum(open_counts_by_key.values()) if open_counts_by_key else 0
    )

    for pos in candidates:
        # Closed positions (eod_close) or direction-less rows always become new inserts.
        if pos.status == "closed" or pos.direction is None:
            new_positions.append(pos)
            if pos.signal_id:
                acted_signal_ids.add(pos.signal_id)
            continue

        key = (pos.contract_id, pos.venue, pos.direction)

        # Count-based guard: block if total open rows (including null-direction legacy
        # entries) already meets the configured ceiling.
        if open_counts_by_key is not None:
            total_open = (open_counts_by_key.get(key, 0)
                          + open_counts_by_key.get((pos.contract_id, pos.venue, ""), 0))
            if total_open >= max_open:
                continue

        existing = existing_by_key.get(key)

        if existing is None:
            # Portfolio-level cap: don't open new positions when the book is full.
            if max_total > 0 and (total_currently_open + len(new_positions)) >= max_total:
                continue
            # No open position for this contract — always insert.
            new_positions.append(pos)
            if pos.signal_id:
                acted_signal_ids.add(pos.signal_id)
        elif settings.paper_allow_add_to_position:
            # VWAP: new_avg = (old_avg * old_qty + fill * new_qty) / (old_qty + new_qty)
            new_qty = existing.net_qty + pos.net_qty
            if new_qty > 0:
                new_avg = (
                    existing.avg_entry_price * existing.net_qty
                    + pos.avg_entry_price * pos.net_qty
                ) / new_qty
            else:
                new_avg = existing.avg_entry_price
            add_tos.append(
                AddToPosition(
                    position_id=existing.position_id,
                    new_net_qty=new_qty,
                    new_avg_entry_price=new_avg,
                )
            )
            if pos.signal_id:
                acted_signal_ids.add(pos.signal_id)
        # else: paper_allow_add_to_position is False and position already exists — skip.

    return new_positions, add_tos, acted_signal_ids


def simulate_paper_trades(
    signals: list[SignalRecord],
    settings: Settings,
) -> tuple[list[PaperOrderRecord], list[PaperPositionRecord]]:
    """
    Create paper orders/positions for actionable signals. Skips hold/reject.
    Unrealized PnL is per contract in price space (0–1) after entry and fees.
    If ``paper_eod_close`` is True, closes each new position at the same mark (realized PnL, demo / stress mode).
    """
    orders: list[PaperOrderRecord] = []
    positions: list[PaperPositionRecord] = []
    now = utc_now()
    slippage = float(settings.paper_slippage_bps)
    fees = float(settings.paper_fees_assumption_bps)
    assumption = settings.paper_assumption_version
    fill_rule = settings.paper_fill_rule
    slippage_model = settings.paper_slippage_model_name
    eod = settings.paper_eod_close
    yes_mid = 0.0  # set per signal

    for signal in signals:
        if signal.decision not in {"enter_long_yes", "enter_long_no"}:
            continue

        side: str = "yes" if signal.decision == "enter_long_yes" else "no"
        yes_mid = float(signal.market_implied_probability)
        if side == "yes":
            raw_fill = _apply_slippage_to_yes_ask(signal.ask_price, slippage)
        else:
            raw_fill = _apply_slippage_to_no_ask(signal.bid_price, slippage)

        qty = _position_qty(raw_fill, settings)

        fee_paid = raw_fill * (fees / 10000.0) * qty
        effective_entry = raw_fill
        no_mid = _no_mark_value(yes_mid)

        if side == "yes":
            gross_mtm = (yes_mid - effective_entry) * qty
        else:
            gross_mtm = (no_mid - effective_entry) * qty
        net_unreal = gross_mtm - fee_paid

        order = PaperOrderRecord(
            signal_id=signal.signal_id,
            run_id=signal.run_id,
            venue=signal.venue,
            contract_id=signal.contract_id,
            side=side,  # type: ignore[arg-type]
            order_type="limit",
            qty=qty,
            limit_price=raw_fill,
            fill_price=raw_fill,
            fill_qty=qty,
            status="filled",
            submitted_at_utc=now,
            filled_at_utc=now,
            fill_rule=fill_rule,
            slippage_assumption_bps=slippage,
            fees_assumption_bps=fees,
            assumption_version=assumption,
            slippage_model_name=slippage_model,
        )
        orders.append(order)

        if eod:
            exit_px = yes_mid if side == "yes" else no_mid
            realized = (exit_px - effective_entry) * qty - fee_paid
            positions.append(
                PaperPositionRecord(
                    position_id=str(uuid4()),
                    run_id=signal.run_id,
                    signal_id=signal.signal_id,
                    venue=signal.venue,
                    contract_id=signal.contract_id,
                    opened_at_utc=now,
                    closed_at_utc=now,
                    net_qty=0.0,
                    avg_entry_price=effective_entry,
                    avg_exit_price=exit_px,
                    realized_pnl=realized,
                    unrealized_pnl=0.0,
                    mark_price=exit_px,
                    last_mark_time_utc=now,
                    status="closed",
                    close_reason="eod_mark",
                    direction=side,  # type: ignore[arg-type]
                )
            )
        else:
            mark_px = yes_mid if side == "yes" else no_mid
            positions.append(
                PaperPositionRecord(
                    position_id=str(uuid4()),
                    run_id=signal.run_id,
                    signal_id=signal.signal_id,
                    venue=signal.venue,
                    contract_id=signal.contract_id,
                    opened_at_utc=now,
                    closed_at_utc=None,
                    net_qty=qty,
                    avg_entry_price=effective_entry,
                    realized_pnl=0.0,
                    unrealized_pnl=net_unreal,
                    mark_price=mark_px,
                    last_mark_time_utc=now,
                    status="open",
                    direction=side,  # type: ignore[arg-type]
                )
            )

    return orders, positions
