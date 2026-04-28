import json
from pathlib import Path

import pytest

from src.connectors.kalshi import KalshiConnector
from src.core.config import Settings
from src.theses.economic_indicators.module import EconomicIndicatorsThesis

FIXTURES = Path(__file__).parent / "fixtures" / "connectors"


def _months() -> list[str]:
    return [f"2024-{m:02d}-01" for m in range(1, 13)]


def _macro_history() -> list[dict]:
    rows: list[dict] = []
    for i, d in enumerate(_months()):
        rows.append({"series": "PPIACO", "date": d, "value": 230.0 + i * 0.4})
        rows.append({"series": "PCEPI", "date": d, "value": 118.0 + i * 0.2})
        rows.append({"series": "UNRATE", "date": d, "value": 4.0 + (i % 3) * 0.1})
        rows.append({"series": "CPIAUCSL", "date": d, "value": 300.0 + i * 0.5})
    return rows


def _fixture_ingest_payload() -> dict:
    kalshi = KalshiConnector()
    markets = json.loads((FIXTURES / "kalshi_markets.json").read_text())
    macro = [
        {"series": "PPIACO", "value": 240.0},
        {"series": "PCEPI", "value": 120.0},
        {"series": "UNRATE", "value": 4.0},
    ]
    return {
        "macro": macro,
        "macro_history": _macro_history(),
        "market": kalshi.parse_markets(markets),
    }


@pytest.fixture
def econ_thesis(tmp_path: Path) -> EconomicIndicatorsThesis:
    settings = Settings(duckdb_path=tmp_path / "db.duckdb", data_dir=tmp_path)
    return EconomicIndicatorsThesis(settings)


def test_econ_pipeline_fixture_chain(monkeypatch: pytest.MonkeyPatch, econ_thesis: EconomicIndicatorsThesis) -> None:
    payload = _fixture_ingest_payload()
    monkeypatch.setattr(econ_thesis, "ingest", lambda: payload)

    raw = econ_thesis.ingest()
    feats = econ_thesis.build_features(raw)
    assert "training_df" in feats
    fc = econ_thesis.forecast(feats)
    assert "model_probability" in fc and "market" in fc
    run_id = "fixture-run-1"
    signals, snaps = econ_thesis.generate_signals(run_id, fc)
    assert len(signals) >= 1
    assert len(snaps) == len(signals)
    orders, pos = econ_thesis.paper_trade(signals)
    assert isinstance(orders, list)
    assert isinstance(pos, list)
