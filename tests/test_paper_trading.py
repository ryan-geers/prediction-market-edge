from pathlib import Path

from src.core.config import Settings
from src.core.schemas import MarketSnapshotRecord, PaperPositionRecord, PositionMark, SignalRecord
from src.core.storage import Storage
from src.pipeline.paper_trading import apply_dedup, apply_exits, simulate_paper_trades


def _base_signal() -> SignalRecord:
    return SignalRecord(
        run_id="r-test",
        thesis_module="economic_indicators",
        venue="KALSHI",
        contract_id="CPI-TEST",
        contract_label="Test CPI",
        model_probability=0.62,
        market_implied_probability=0.50,
        edge_bps=1200.0,
        bid_price=0.45,
        ask_price=0.55,
        spread_bps=200.0,
        vig_adjusted_threshold_bps=300.0,
        decision="enter_long_yes",
        decision_reason="test",
        model_version="m1",
        feature_set_version="f1",
        assumption_version="paper_exec_v1",
    )


def _open_position(
    contract_id: str = "CPI-TEST",
    venue: str = "KALSHI",
    direction: str = "yes",
    avg_entry_price: float = 0.50,
    net_qty: float = 50.0,
    unrealized_pnl: float = 0.0,
) -> PaperPositionRecord:
    return PaperPositionRecord(
        run_id="r-test",
        signal_id="sig-test",
        venue=venue,
        contract_id=contract_id,
        net_qty=net_qty,
        avg_entry_price=avg_entry_price,
        unrealized_pnl=unrealized_pnl,
        mark_price=avg_entry_price,
        status="open",
        direction=direction,  # type: ignore[arg-type]
    )


def _snapshot(
    contract_id: str = "CPI-TEST",
    venue: str = "KALSHI",
    mid_price: float = 0.50,
) -> MarketSnapshotRecord:
    return MarketSnapshotRecord(
        venue=venue,
        contract_id=contract_id,
        best_bid=mid_price - 0.01,
        best_ask=mid_price + 0.01,
        mid_price=mid_price,
        spread_bps=200.0,
    )


def test_paper_trading_enters_on_edge_signal():
    s = _base_signal()
    settings = Settings()
    orders, pos = simulate_paper_trades([s], settings)
    assert len(orders) == 1
    assert len(pos) == 1
    assert orders[0].side == "yes"
    assert orders[0].status == "filled"
    assert orders[0].assumption_version == settings.paper_assumption_version
    assert pos[0].run_id == "r-test"
    assert pos[0].signal_id == s.signal_id
    assert pos[0].status == "open"


def test_simulate_paper_trades_sets_direction():
    s_yes = _base_signal()  # decision="enter_long_yes"
    s_no = _base_signal().model_copy(
        update={"decision": "enter_long_no", "edge_bps": -1200.0, "contract_id": "CPI-NO"}
    )
    settings = Settings()
    _, positions = simulate_paper_trades([s_yes, s_no], settings)
    assert positions[0].direction == "yes"
    assert positions[1].direction == "no"


def test_paper_trading_skips_hold():
    s = _base_signal()
    s = s.model_copy(update={"decision": "hold"})
    settings = Settings()
    orders, pos = simulate_paper_trades([s], settings)
    assert orders == [] and pos == []


def test_paper_eod_close_realizes_pnl():
    s = _base_signal()
    settings = Settings()
    settings = settings.model_copy(update={"paper_eod_close": True})
    orders, pos = simulate_paper_trades([s], settings)
    assert len(pos) == 1
    assert pos[0].status == "closed"
    assert pos[0].close_reason == "eod_mark"


def test_mark_open_positions_updates_after_second_run(tmp_path: Path) -> None:
    """
    Integration: simulate two pipeline runs.
    The position opened in run-1 should have its mark_price updated after run-2
    calls mark_open_positions with a new price.
    """
    settings = Settings()
    st = Storage(tmp_path / "t.duckdb")

    # Run 1: enter a YES position on CPI-TEST at fill price ~0.55 (ask + slippage)
    s1 = _base_signal()
    s1 = s1.model_copy(update={"run_id": "run-1"})
    _, positions_run1 = simulate_paper_trades([s1], settings)
    assert len(positions_run1) == 1
    st.insert_positions(positions_run1)

    run1_pos = positions_run1[0]
    entry_mark = run1_pos.mark_price  # mid at entry (0.50)

    # Run 2: market has moved — YES now trades at 0.65 mid
    new_mid = 0.65
    marks = [PositionMark(contract_id="CPI-TEST", venue="KALSHI", mark_price=new_mid)]
    updated = st.mark_open_positions(marks)
    st.close()

    assert updated == 1, "Expected exactly the one open position row to be updated"

    import duckdb
    con = duckdb.connect(str(tmp_path / "t.duckdb"))
    row = con.execute(
        "SELECT mark_price, unrealized_pnl FROM paper_positions WHERE position_id = ?",
        [run1_pos.position_id],
    ).fetchone()
    con.close()

    assert row is not None
    assert abs(row[0] - new_mid) < 1e-9, "mark_price should be updated to new mid"
    # unrealized_pnl = (new_mid - avg_entry_price) * net_qty
    expected_unrealized = (new_mid - run1_pos.avg_entry_price) * run1_pos.net_qty
    assert abs(row[1] - expected_unrealized) < 1e-6
    # Sanity: the mark genuinely changed
    assert abs(row[0] - entry_mark) > 1e-9, "mark should differ from entry-day snapshot"


def test_no_position_remark_uses_no_mid_for_stop_loss(tmp_path: Path) -> None:
    """A persisted NO position should be re-marked with 1 - YES mid before exits."""
    settings = Settings(paper_stop_loss_pct=0.15, paper_exit_on_flip=False)
    st = Storage(tmp_path / "t.duckdb")

    no_signal = _base_signal().model_copy(
        update={
            "decision": "enter_long_no",
            "edge_bps": -1200.0,
            "bid_price": 0.45,
            "market_implied_probability": 0.50,
            "run_id": "run-no",
        }
    )
    _, positions_run1 = simulate_paper_trades([no_signal], settings)
    assert len(positions_run1) == 1
    assert positions_run1[0].direction == "no"
    st.insert_positions(positions_run1)

    # YES mid rallies to 0.80, so the NO mark falls to 0.20 and breaches the stop.
    snap = _snapshot(mid_price=0.80)
    st.mark_open_positions(
        [PositionMark(contract_id="CPI-TEST", venue="KALSHI", mark_price=snap.mid_price)]
    )
    open_positions = st.get_open_positions()
    st.close()

    assert len(open_positions) == 1
    assert abs(open_positions[0].mark_price - 0.20) < 1e-9
    assert open_positions[0].unrealized_pnl < 0

    closes = apply_exits(open_positions, [], [snap], settings)
    assert len(closes) == 1
    assert closes[0].close_reason == "stop_loss"
    assert abs(closes[0].avg_exit_price - 0.20) < 1e-9


# ── Phase 2: apply_exits tests ─────────────────────────────────────────────────

def test_apply_exits_flip_closes_yes_position_on_no_signal():
    """Rule A: YES position is closed when the current signal flips to NO."""
    settings = Settings(edge_threshold_bps=300, paper_exit_on_flip=True)
    pos = _open_position(direction="yes", avg_entry_price=0.55, net_qty=45.0)
    # Signal now strongly favors NO: edge_bps well below -threshold
    flip_signal = _base_signal().model_copy(
        update={"decision": "enter_long_no", "edge_bps": -800.0, "market_implied_probability": 0.30}
    )
    snap = _snapshot(mid_price=0.30)
    closes = apply_exits([pos], [flip_signal], [snap], settings)
    assert len(closes) == 1
    assert closes[0].position_id == pos.position_id
    assert closes[0].close_reason == "signal_flip"
    # exit at yes_mid = 0.30
    assert abs(closes[0].avg_exit_price - 0.30) < 1e-9


def test_apply_exits_flip_closes_no_position_on_yes_signal():
    """Rule A: NO position is closed when the current signal flips to YES."""
    settings = Settings(edge_threshold_bps=300, paper_exit_on_flip=True)
    pos = _open_position(direction="no", avg_entry_price=0.55, net_qty=45.0)
    # Signal now strongly favors YES: edge_bps well above +threshold
    flip_signal = _base_signal().model_copy(
        update={"decision": "enter_long_yes", "edge_bps": 800.0, "market_implied_probability": 0.70}
    )
    snap = _snapshot(mid_price=0.70)
    closes = apply_exits([pos], [flip_signal], [snap], settings)
    assert len(closes) == 1
    assert closes[0].close_reason == "signal_flip"
    # NO exit price = 1 - yes_mid = 0.30
    assert abs(closes[0].avg_exit_price - 0.30) < 1e-9


def test_apply_exits_no_flip_when_signal_unchanged():
    """Rule A should NOT fire when edge is still on the same side as the position."""
    settings = Settings(edge_threshold_bps=300, paper_exit_on_flip=True)
    pos = _open_position(direction="yes", avg_entry_price=0.50, net_qty=50.0)
    same_signal = _base_signal()  # edge_bps=1200, still favoring YES
    snap = _snapshot(mid_price=0.55)
    closes = apply_exits([pos], [same_signal], [snap], settings)
    assert closes == []


def test_apply_exits_stop_loss_fires_at_threshold():
    """Rule B: position closes when unrealised loss exceeds paper_stop_loss_pct."""
    # cost_basis = 0.50 * 50 = 25; -20% loss = unrealized_pnl = -5.0
    settings = Settings(paper_stop_loss_pct=0.15, paper_exit_on_flip=False)
    pos = _open_position(
        direction="yes",
        avg_entry_price=0.50,
        net_qty=50.0,
        unrealized_pnl=-5.0,  # -20% of $25 cost basis — exceeds 15% threshold
    )
    snap = _snapshot(mid_price=0.40)
    closes = apply_exits([pos], [], [snap], settings)
    assert len(closes) == 1
    assert closes[0].close_reason == "stop_loss"


def test_apply_exits_stop_loss_does_not_fire_below_threshold():
    """Rule B: no close when loss is smaller than the threshold."""
    settings = Settings(paper_stop_loss_pct=0.15, paper_exit_on_flip=False)
    pos = _open_position(
        direction="yes",
        avg_entry_price=0.50,
        net_qty=50.0,
        unrealized_pnl=-1.0,  # -4% of $25 cost basis — well within tolerance
    )
    snap = _snapshot(mid_price=0.48)
    closes = apply_exits([pos], [], [snap], settings)
    assert closes == []


def test_apply_exits_stop_loss_disabled_by_default():
    """paper_stop_loss_pct=None means Rule B never fires."""
    settings = Settings(paper_stop_loss_pct=None, paper_exit_on_flip=False)
    pos = _open_position(unrealized_pnl=-999.0)  # absurdly large loss
    closes = apply_exits([pos], [], [], settings)
    assert closes == []


def test_apply_exits_skips_already_closed_positions():
    """Closed positions must be ignored by apply_exits."""
    settings = Settings(paper_exit_on_flip=True)
    pos = _open_position(direction="yes")
    closed_pos = pos.model_copy(update={"status": "closed"})
    flip_signal = _base_signal().model_copy(
        update={"decision": "enter_long_no", "edge_bps": -800.0}
    )
    closes = apply_exits([closed_pos], [flip_signal], [], settings)
    assert closes == []


def test_apply_exits_exit_on_flip_disabled():
    """Setting paper_exit_on_flip=False prevents Rule A from firing."""
    settings = Settings(edge_threshold_bps=300, paper_exit_on_flip=False)
    pos = _open_position(direction="yes")
    flip_signal = _base_signal().model_copy(
        update={"decision": "enter_long_no", "edge_bps": -800.0}
    )
    closes = apply_exits([pos], [flip_signal], [], settings)
    assert closes == []


# ── Phase 3: apply_dedup tests ─────────────────────────────────────────────────

def _make_position(
    position_id: str = "pos-1",
    contract_id: str = "CPI-TEST",
    venue: str = "KALSHI",
    direction: str = "yes",
    avg_entry_price: float = 0.50,
    net_qty: float = 50.0,
    status: str = "open",
) -> PaperPositionRecord:
    return PaperPositionRecord(
        position_id=position_id,
        run_id="r-test",
        signal_id="sig-test",
        venue=venue,
        contract_id=contract_id,
        net_qty=net_qty,
        avg_entry_price=avg_entry_price,
        status=status,  # type: ignore[arg-type]
        direction=direction,  # type: ignore[arg-type]
    )


def test_apply_dedup_disabled_by_default():
    """When paper_allow_add_to_position=False all candidates pass through unchanged."""
    settings = Settings(paper_allow_add_to_position=False)
    candidate = _make_position()
    existing = _make_position(position_id="existing")
    existing_by_key = {("CPI-TEST", "KALSHI", "yes"): existing}

    new_positions, add_tos = apply_dedup([candidate], existing_by_key, settings)

    assert new_positions == [candidate]
    assert add_tos == []


def test_apply_dedup_no_existing_inserts_new():
    """When no existing position matches the candidate it becomes a new insert."""
    settings = Settings(paper_allow_add_to_position=True)
    candidate = _make_position()

    new_positions, add_tos = apply_dedup([candidate], {}, settings)

    assert new_positions == [candidate]
    assert add_tos == []


def test_apply_dedup_same_contract_same_direction_merges():
    """Two YES signals on the same contract produce one VWAP add-to, not a new row."""
    settings = Settings(paper_allow_add_to_position=True)
    existing = _make_position(position_id="existing", avg_entry_price=0.50, net_qty=50.0)
    candidate = _make_position(position_id="new-fill", avg_entry_price=0.60, net_qty=40.0)
    existing_by_key = {("CPI-TEST", "KALSHI", "yes"): existing}

    new_positions, add_tos = apply_dedup([candidate], existing_by_key, settings)

    assert new_positions == []
    assert len(add_tos) == 1

    op = add_tos[0]
    assert op.position_id == "existing"
    assert op.new_net_qty == 90.0
    # VWAP: (0.50*50 + 0.60*40) / 90 = (25 + 24) / 90 = 49/90
    expected_avg = (0.50 * 50.0 + 0.60 * 40.0) / 90.0
    assert abs(op.new_avg_entry_price - expected_avg) < 1e-9


def test_apply_dedup_different_direction_creates_new_row():
    """A NO candidate on a contract already held as YES is a separate new insert."""
    settings = Settings(paper_allow_add_to_position=True)
    existing_yes = _make_position(position_id="yes-pos", direction="yes")
    candidate_no = _make_position(position_id="no-cand", direction="no")
    existing_by_key = {("CPI-TEST", "KALSHI", "yes"): existing_yes}

    new_positions, add_tos = apply_dedup([candidate_no], existing_by_key, settings)

    assert new_positions == [candidate_no]
    assert add_tos == []


def test_apply_dedup_closed_candidate_passes_through():
    """Closed candidates (eod_close) always become new inserts regardless of dedup."""
    settings = Settings(paper_allow_add_to_position=True)
    existing = _make_position(position_id="existing")
    closed_candidate = _make_position(position_id="eod-closed", status="closed")
    existing_by_key = {("CPI-TEST", "KALSHI", "yes"): existing}

    new_positions, add_tos = apply_dedup([closed_candidate], existing_by_key, settings)

    assert new_positions == [closed_candidate]
    assert add_tos == []


def test_apply_dedup_multiple_contracts_independent():
    """Two candidates on different contracts are deduped independently."""
    settings = Settings(paper_allow_add_to_position=True)
    ex_a = _make_position(position_id="ex-a", contract_id="CPI-A", avg_entry_price=0.40, net_qty=25.0)
    ex_b = _make_position(position_id="ex-b", contract_id="CPI-B", avg_entry_price=0.60, net_qty=10.0)
    cand_a = _make_position(position_id="c-a", contract_id="CPI-A", avg_entry_price=0.45, net_qty=20.0)
    cand_c = _make_position(position_id="c-c", contract_id="CPI-C", avg_entry_price=0.55, net_qty=30.0)
    existing_by_key = {
        ("CPI-A", "KALSHI", "yes"): ex_a,
        ("CPI-B", "KALSHI", "yes"): ex_b,
    }

    new_positions, add_tos = apply_dedup([cand_a, cand_c], existing_by_key, settings)

    # CPI-A merges, CPI-C is new (no existing), CPI-B untouched
    assert len(new_positions) == 1
    assert new_positions[0].contract_id == "CPI-C"
    assert len(add_tos) == 1
    assert add_tos[0].position_id == "ex-a"
