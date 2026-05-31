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


def test_flip_without_unreliable_skip_uses_bid_not_ask_proxy():
    settings = Settings(paper_exit_on_flip=True, paper_skip_exit_on_unreliable_quote=False)
    pos = PaperPositionRecord(
        run_id="r",
        signal_id="s",
        venue="kalshi",
        contract_id="KXECONSTATU3-26AUG-T5.0",
        net_qty=100.0,
        avg_entry_price=0.50,
        status="open",
        direction="yes",
    )
    signal = _flip_signal().model_copy(
        update={
            "ask_price": 0.10,
            "decision_reason": "contract_type=unemployment;quote_quality=one_sided_ask_proxy",
        }
    )
    snap = _broken_snap().model_copy(
        update={"best_ask": 0.10, "last_trade": None, "mid_price": 0.10}
    )

    closes = apply_exits([pos], [signal], [snap], settings)

    assert len(closes) == 1
    assert closes[0].avg_exit_price == 0.0
    assert closes[0].realized_pnl == -50.0
