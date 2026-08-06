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


def test_signal_horizon_gate_blocks_non_matching_months(tmp_path: Path) -> None:
    """One-step CPI forecast must not score distant contract months (JUL vs NOV)."""
    settings = Settings(
        duckdb_path=tmp_path / "db.duckdb",
        data_dir=tmp_path,
        edge_threshold_bps=300,
        signal_block_long_no_when_model_favors_yes=False,
    )
    thesis = EconomicIndicatorsThesis(settings)
    # Pred 0.5% vs threshold 0.3% → strong YES edge vs mid≈0.40.
    fc = {
        "market": [
            {
                "venue": "kalshi",
                "contract_id": "KXCPI-26JUL-T0.3",
                "label": "Jul CPI > 0.3%",
                "best_bid": 0.38,
                "best_ask": 0.42,
                "last_trade": 0.40,
                "contract_type": "cpi",
                "threshold": 0.3,
                "event_month": "2026-07-01",
            },
            {
                "venue": "kalshi",
                "contract_id": "KXCPI-26NOV-T0.3",
                "label": "Nov CPI > 0.3%",
                "best_bid": 0.38,
                "best_ask": 0.42,
                "last_trade": 0.40,
                "contract_type": "cpi",
                "threshold": 0.3,
                "event_month": "2026-11-01",
            },
        ],
        "predicted_cpi_mom_pct": 0.5,
        "cpi_mom_threshold_pct": 0.3,
        "validation_rmse": 0.2,
        "walk_forward_val_rmse": 0.2,
        "macro_history_count": 48,
        "model_healthy": True,
        "cpi_forecast_target_month": "2026-07-01",
        "un_reg": None,
        "un_healthy": False,
    }
    signals, _ = thesis.generate_signals("horizon-run", fc)
    by_id = {s.contract_id: s for s in signals}
    assert by_id["KXCPI-26JUL-T0.3"].decision == "enter_long_yes"
    assert by_id["KXCPI-26NOV-T0.3"].decision == "hold"
    assert "blocked_by_horizon_mismatch" in by_id["KXCPI-26NOV-T0.3"].decision_reason
    # Same scalar prediction was still attached to both before the gate — NOV must not enter.
    assert by_id["KXCPI-26JUL-T0.3"].model_probability == pytest.approx(
        by_id["KXCPI-26NOV-T0.3"].model_probability
    )


def test_signal_horizon_gate_unemployment(tmp_path: Path) -> None:
    """Unemployment one-step pred must not enter distant month ladders."""
    from types import SimpleNamespace

    settings = Settings(
        duckdb_path=tmp_path / "db.duckdb",
        data_dir=tmp_path,
        edge_threshold_bps=300,
        signal_block_long_no_when_model_favors_yes=False,
    )
    thesis = EconomicIndicatorsThesis(settings)
    un_reg = SimpleNamespace(prediction=4.5, rmse=0.15, walk_forward_val_rmse=0.2)
    fc = {
        "market": [
            {
                "venue": "kalshi",
                "contract_id": "KXU3-26JUL-T4.2",
                "label": "Jul U3 > 4.2%",
                "best_bid": 0.40,
                "best_ask": 0.44,
                "last_trade": 0.42,
                "contract_type": "unemployment",
                "threshold": 4.2,
                "event_month": "2026-07-01",
            },
            {
                "venue": "kalshi",
                "contract_id": "KXU3-26NOV-T4.2",
                "label": "Nov U3 > 4.2%",
                "best_bid": 0.40,
                "best_ask": 0.44,
                "last_trade": 0.42,
                "contract_type": "unemployment",
                "threshold": 4.2,
                "event_month": "2026-11-01",
            },
        ],
        "validation_rmse": 0.2,
        "walk_forward_val_rmse": 0.2,
        "macro_history_count": 48,
        "model_healthy": True,
        "un_reg": un_reg,
        "un_healthy": True,
        "un_forecast_target_month": "2026-07-01",
    }
    signals, _ = thesis.generate_signals("un-horizon", fc)
    by_id = {s.contract_id: s for s in signals}
    assert by_id["KXU3-26JUL-T4.2"].decision == "enter_long_yes"
    assert by_id["KXU3-26NOV-T4.2"].decision == "hold"
    assert "blocked_by_horizon_mismatch" in by_id["KXU3-26NOV-T4.2"].decision_reason


def test_forecast_target_month_is_one_step_ahead(tmp_path: Path) -> None:
    """forecast() exposes target month = last training release_date + 1 month."""
    import pandas as pd

    from src.theses.economic_indicators.module import _forecast_target_month_from_training

    df = pd.DataFrame(
        {
            "release_date": pd.to_datetime(["2026-05-01", "2026-06-01"]),
            "ppi": [240.0, 241.0],
            "pcepi": [120.0, 121.0],
            "unrate": [4.1, 4.2],
            "cpi_mom_next": [0.2, 0.3],
        }
    )
    target = _forecast_target_month_from_training(df)
    assert target is not None
    assert (target.year, target.month) == (2026, 7)
