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


def test_stale_last_outside_book_does_not_flip_to_long_no(tmp_path: Path) -> None:
    """A last trade outside the posted book must not open the opposite side.

    Mirrors KXECONSTATU3-26NOV-T4.2 on 2026-10-29: YES 0.21/0.30, last 0.89,
    model P(YES) ≈ 0.45. Using 0.89 as fair value emits enter_long_no and
    closes an existing long YES. The live touch still prices YES below the model.
    """
    from datetime import datetime, timezone

    from src.theses.economic_indicators.unemployment_model import UnrateRegressionResult

    settings = Settings(
        duckdb_path=tmp_path / "db.duckdb",
        data_dir=tmp_path,
        edge_threshold_bps=300,
        signal_block_long_no_when_model_favors_yes=False,
    )
    thesis = EconomicIndicatorsThesis(settings)
    now = datetime.now(timezone.utc)
    un_reg = UnrateRegressionResult(
        prediction=4.192,
        rmse=0.1151,
        mae=0.09,
        train_rmse=0.1,
        train_mae=0.08,
        walk_forward_val_rmse=0.1121,
        walk_forward_val_mae=0.09,
        n_train=100,
        n_val=20,
        training_start=now,
        training_end=now,
    )
    fc = {
        "market": [
            {
                "venue": "kalshi",
                "contract_id": "KXECONSTATU3-26NOV-T4.2",
                "label": "Unemployment exactly 4.2%",
                "best_bid": 0.21,
                "best_ask": 0.30,
                "last_trade": 0.89,
                "contract_type": "unemployment",
                "threshold": 4.2,
                "is_stub": False,
            }
        ],
        "model_healthy": True,
        "un_reg": un_reg,
        "un_healthy": True,
        "macro_history_count": 27680,
    }
    signals, snaps = thesis.generate_signals("r-stale-last", fc)
    assert len(signals) == 1
    sig = signals[0]
    assert sig.decision == "hold"
    assert sig.edge_bps == 0.0
    assert "quote_unusable" in sig.decision_reason
    assert "unusable_wide_spread" in sig.decision_reason
    assert snaps[0].best_bid == 0.21
    assert snaps[0].best_ask == 0.30
    assert snaps[0].last_trade == 0.89


def test_signal_block_long_no_when_model_favors_yes(tmp_path: Path) -> None:
    """Suppress long NO when model P(YES) > 50% (avoid fading a YES modal outcome)."""
    settings = Settings(
        duckdb_path=tmp_path / "db.duckdb",
        data_dir=tmp_path,
        signal_block_long_no_when_model_favors_yes=True,
        edge_threshold_bps=300,
    )
    thesis = EconomicIndicatorsThesis(settings)
    run_id = "r-no-fade"
    fc = {
        "market": [
            {
                "venue": "KALSHI",
                "contract_id": "SYN-CPI-STUB",
                "label": "synthetic",
                "best_bid": 0.97,
                "best_ask": 0.99,
                "last_trade": 0.98,
                "contract_type": "cpi",
                "is_stub": False,
            }
        ],
        "model_probability": 0.81,
        "predicted_cpi_mom_pct": 0.4,
        "validation_rmse": 0.5,
        "walk_forward_val_rmse": 0.5,
        "macro_history_count": 100,
        "model_healthy": True,
        "un_reg": None,
        "un_healthy": False,
    }
    signals, _ = thesis.generate_signals(run_id, fc)
    assert len(signals) == 1
    assert signals[0].decision == "hold"
    assert "blocked_by_no_fade_policy" in signals[0].decision_reason
