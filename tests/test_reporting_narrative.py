"""Sanity checks for weekly-digest / position plain-English copy."""

from datetime import datetime, timedelta, timezone

from src.core.schemas import SignalRecord
from src.core.storage import Storage
from src.pipeline.reporting import _position_rationale, _weekly_payload


def test_unemployment_yes_narrative_when_point_below_threshold() -> None:
    """Long YES while the point forecast is below strike — must not say 'above the threshold'."""
    reason = (
        "contract_type=unemployment;model_vs_mid_edge_bps=2600;"
        "pred_unrate=4.328;threshold=4.6;val_rmse=0.89"
    )
    out = _position_rationale(
        "enter_long_yes",
        0.286,
        0.025,
        2600.0,
        reason,
    )
    assert "above the 4.6" not in out.lower()
    assert "below the 4.6" in out.lower() or "below the 4.6%" in out.lower()
    assert "tail" in out.lower() or "underpriced" in out.lower()


def test_unemployment_yes_narrative_when_point_above_threshold() -> None:
    reason = (
        "contract_type=unemployment;model_vs_mid_edge_bps=500;"
        "pred_unrate=5.1;threshold=4.6;val_rmse=0.89"
    )
    out = _position_rationale(
        "enter_long_yes",
        0.55,
        0.40,
        1500.0,
        reason,
    )
    assert "above the 4.6" in out.lower()


def test_unemployment_narrative_parses_json_decision_reason() -> None:
    reason = (
        '{"contract_type":"unemployment","model_vs_mid_edge_bps":2600,'
        '"pred_unrate":4.328,"threshold":4.6,"val_rmse":0.89}'
    )
    out = _position_rationale(
        "enter_long_yes",
        0.286,
        0.025,
        2600.0,
        reason,
    )
    assert "above the 4.6" not in out.lower()
    assert "below the 4.6" in out.lower() or "below the 4.6%" in out.lower()
    assert "tail" in out.lower() or "underpriced" in out.lower()


def test_weekly_payload_detects_json_stub_decision_reason(tmp_path) -> None:
    st = Storage(tmp_path / "t.duckdb")
    event_time = datetime.now(timezone.utc)
    st.insert_signals([
        SignalRecord(
            run_id="r",
            thesis_module="economic_indicators",
            venue="kalshi",
            contract_id="KXSTUB",
            contract_label="Stub",
            event_time_utc=event_time,
            model_probability=0.6,
            market_implied_probability=0.5,
            edge_bps=1000.0,
            bid_price=0.49,
            ask_price=0.51,
            spread_bps=200.0,
            vig_adjusted_threshold_bps=300.0,
            decision="hold",
            decision_reason='{"contract_type":"cpi","data_source":"kalshi_stub"}',
            model_version="m",
            feature_set_version="f",
            assumption_version="paper_exec_v1",
        )
    ])

    payload = _weekly_payload(st.con, event_time - timedelta(minutes=1))
    st.close()

    assert payload["stub_contract_count"] == 1
