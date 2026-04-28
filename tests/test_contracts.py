import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from src.core.schemas import (
    MarketSnapshotRecord,
    ModelForecastRecord,
    PaperOrderRecord,
    PaperPositionRecord,
    RunManifest,
    SignalRecord,
    utc_now,
)

CONTRACTS = Path(__file__).resolve().parents[1] / "contracts" / "json"


def _checker() -> FormatChecker:
    return FormatChecker()


@pytest.mark.parametrize(
    "model, stem",
    [
        (SignalRecord, "signal_record"),
        (PaperOrderRecord, "paper_order"),
        (PaperPositionRecord, "paper_position"),
        (MarketSnapshotRecord, "market_snapshot"),
        (RunManifest, "run_manifest"),
        (ModelForecastRecord, "model_forecast"),
    ],
)
def test_committed_schema_matches_pydantic_model(model, stem: str) -> None:
    """Regenerate with: python scripts/export_contract_schemas.py"""
    path = CONTRACTS / f"{stem}.schema.json"
    committed = json.loads(path.read_text(encoding="utf-8"))
    assert committed == model.model_json_schema()


def test_signal_instance_validates_against_schema() -> None:
    schema = json.loads((CONTRACTS / "signal_record.schema.json").read_text(encoding="utf-8"))
    now = utc_now()
    rec = SignalRecord(
        run_id="r1",
        thesis_module="economic_indicators",
        venue="KALSHI",
        contract_id="C1",
        contract_label="Lab",
        event_time_utc=now,
        ingested_at_utc=now,
        model_probability=0.55,
        market_implied_probability=0.5,
        edge_bps=500.0,
        bid_price=0.48,
        ask_price=0.52,
        spread_bps=80.0,
        vig_adjusted_threshold_bps=300.0,
        decision="hold",
        decision_reason="test",
        model_version="m",
        feature_set_version="f",
        assumption_version="a",
    )
    inst = rec.model_dump(mode="json")
    Draft202012Validator(schema, format_checker=_checker()).validate(inst)


def test_paper_order_instance_validates() -> None:
    schema = json.loads((CONTRACTS / "paper_order.schema.json").read_text(encoding="utf-8"))
    rec = PaperOrderRecord(
        signal_id="s",
        run_id="r",
        venue="K",
        contract_id="c",
        side="yes",
        order_type="limit",
        qty=1.0,
    )
    Draft202012Validator(schema, format_checker=_checker()).validate(rec.model_dump(mode="json"))
