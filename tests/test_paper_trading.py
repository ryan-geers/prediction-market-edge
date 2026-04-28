from src.core.config import Settings
from src.core.schemas import SignalRecord
from src.pipeline.paper_trading import simulate_paper_trades


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
