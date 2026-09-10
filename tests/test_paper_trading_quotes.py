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


def test_no_flip_blocked_on_pinned_dollar_ask():
    """Long NO must not close at $0 just because Kalshi pinned yes_ask=1.00."""
    settings = Settings(
        paper_exit_on_flip=True,
        paper_skip_exit_on_unreliable_quote=True,
        edge_threshold_bps=300,
    )
    pos = PaperPositionRecord(
        run_id="r",
        signal_id="s",
        venue="kalshi",
        contract_id="KXCPI-26SEP-T0.2",
        net_qty=103.91,
        avg_entry_price=0.2406,
        status="open",
        direction="no",
    )
    sig = SignalRecord(
        run_id="r",
        thesis_module="economic_indicators",
        venue="kalshi",
        contract_id="KXCPI-26SEP-T0.2",
        contract_label="Sep CPI T0.2",
        model_probability=0.91,
        market_implied_probability=0.97,
        edge_bps=400.0,  # flip NO → YES
        bid_price=0.74,
        ask_price=1.0,
        spread_bps=2988.0,
        vig_adjusted_threshold_bps=300.0,
        decision="enter_long_yes",
        decision_reason='{"contract_type":"cpi","quote_quality":"bid_only_pinned_ask"}',
        model_version="m",
        feature_set_version="f",
        assumption_version="paper_exec_v1",
    )
    snap = MarketSnapshotRecord(
        venue="kalshi",
        contract_id="KXCPI-26SEP-T0.2",
        best_bid=0.74,
        best_ask=1.0,
        last_trade=0.97,
        mid_price=0.97,
        spread_bps=2988.0,
    )
    closes = apply_exits([pos], [sig], [snap], settings)
    assert closes == []


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
