"""Paper exits blocked on broken quotes."""

from src.core.config import Settings
from src.core.schemas import MarketSnapshotRecord, PaperPositionRecord, SignalRecord
from src.pipeline.paper_trading import apply_exits


def _flip_signal() -> SignalRecord:
    return SignalRecord(
        run_id="r",
        thesis_module="economic_indicators",
        venue="kalshi",
        contract_id="KXECONSTATU3-26AUG-T5.0",
        contract_label="U3 T5",
        model_probability=0.09,
        market_implied_probability=0.0,
        edge_bps=-5000.0,
        bid_price=0.0,
        ask_price=1.0,
        spread_bps=0.0,
        vig_adjusted_threshold_bps=300.0,
        decision="hold",
        decision_reason="contract_type=unemployment;quote_unusable=true;quote_quality=unusable_empty_book",
        model_version="m",
        feature_set_version="f",
        assumption_version="paper_exec_v1",
    )


def _broken_snap() -> MarketSnapshotRecord:
    return MarketSnapshotRecord(
        venue="kalshi",
        contract_id="KXECONSTATU3-26AUG-T5.0",
        best_bid=0.0,
        best_ask=1.0,
        last_trade=0.0,
        mid_price=0.5,
        spread_bps=0.0,
    )


def test_flip_blocked_on_empty_book():
    settings = Settings(paper_exit_on_flip=True, paper_skip_exit_on_unreliable_quote=True)
    pos = PaperPositionRecord(
        run_id="r",
        signal_id="s",
        venue="kalshi",
        contract_id="KXECONSTATU3-26AUG-T5.0",
        net_qty=100.0,
        avg_entry_price=0.10,
        status="open",
        direction="yes",
    )
    closes = apply_exits([pos], [_flip_signal()], [_broken_snap()], settings)
    assert closes == []


def test_no_flip_blocked_on_bid_only_ask_zero():
    """Long NO must not exit at 1.0 when Kalshi reports yes_ask=0."""
    settings = Settings(paper_exit_on_flip=True, paper_skip_exit_on_unreliable_quote=True, edge_threshold_bps=300.0)
    pos = PaperPositionRecord(
        run_id="r",
        signal_id="s",
        venue="kalshi",
        contract_id="KXECONSTATU3-26AUG-T5.0",
        net_qty=50.0,
        avg_entry_price=0.40,
        status="open",
        direction="no",
        unrealized_pnl=0.0,
        mark_price=0.60,
    )
    # Flip: long NO closes when edge_bps > threshold (model favors YES).
    sig = SignalRecord(
        run_id="r",
        thesis_module="economic_indicators",
        venue="kalshi",
        contract_id="KXECONSTATU3-26AUG-T5.0",
        contract_label="U3 T5",
        model_probability=0.80,
        market_implied_probability=0.55,
        edge_bps=2500.0,
        bid_price=0.55,
        ask_price=0.0,
        spread_bps=0.0,
        vig_adjusted_threshold_bps=300.0,
        decision="enter_long_yes",
        decision_reason='{"quote_quality":"unusable_bid_only"}',
        model_version="m",
        feature_set_version="f",
        assumption_version="paper_exec_v1",
    )
    snap = MarketSnapshotRecord(
        venue="kalshi",
        contract_id="KXECONSTATU3-26AUG-T5.0",
        best_bid=0.55,
        best_ask=0.0,
        last_trade=0.50,
        mid_price=0.50,
        spread_bps=0.0,
    )
    closes = apply_exits([pos], [sig], [snap], settings)
    assert closes == []
