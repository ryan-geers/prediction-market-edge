import pytest

from src.theses.economic_indicators.model import (
    build_training_frame,
    build_training_frame_from_history,
    mom_percent_to_yes_probability,
    train_validate_predict,
)


def test_regression_training_and_prediction():
    macro = {"PPIACO": 246.5, "UNRATE": 4.1, "PCEPI": 123.7}
    frame = build_training_frame(macro, periods=30)
    result = train_validate_predict(frame)

    assert len(frame) == 30
    assert result.training_start < result.training_end
    assert result.rmse >= 0
    assert result.mae >= 0
    assert -0.5 < result.prediction < 2.0  # m/m in percent points


def test_build_training_frame_from_history():
    history = [
        {"series": "PPIACO", "value": 242.0, "date": "2025-01-01"},
        {"series": "PCEPI", "value": 121.8, "date": "2025-01-01"},
        {"series": "UNRATE", "value": 4.3, "date": "2025-01-01"},
        {"series": "PPIACO", "value": 243.5, "date": "2025-02-01"},
        {"series": "PCEPI", "value": 122.0, "date": "2025-02-01"},
        {"series": "UNRATE", "value": 4.2, "date": "2025-02-01"},
        {"series": "PPIACO", "value": 244.0, "date": "2025-03-01"},
        {"series": "PCEPI", "value": 122.2, "date": "2025-03-01"},
        {"series": "UNRATE", "value": 4.2, "date": "2025-03-01"},
        {"series": "PPIACO", "value": 244.8, "date": "2025-04-01"},
        {"series": "PCEPI", "value": 122.4, "date": "2025-04-01"},
        {"series": "UNRATE", "value": 4.1, "date": "2025-04-01"},
        {"series": "PPIACO", "value": 245.5, "date": "2025-05-01"},
        {"series": "PCEPI", "value": 122.7, "date": "2025-05-01"},
        {"series": "UNRATE", "value": 4.1, "date": "2025-05-01"},
        {"series": "PPIACO", "value": 245.9, "date": "2025-06-01"},
        {"series": "PCEPI", "value": 123.0, "date": "2025-06-01"},
        {"series": "UNRATE", "value": 4.0, "date": "2025-06-01"},
        {"series": "PPIACO", "value": 246.1, "date": "2025-07-01"},
        {"series": "PCEPI", "value": 123.2, "date": "2025-07-01"},
        {"series": "UNRATE", "value": 4.0, "date": "2025-07-01"},
        {"series": "PPIACO", "value": 246.3, "date": "2025-08-01"},
        {"series": "PCEPI", "value": 123.3, "date": "2025-08-01"},
        {"series": "UNRATE", "value": 4.0, "date": "2025-08-01"},
        {"series": "PPIACO", "value": 246.4, "date": "2025-09-01"},
        {"series": "PCEPI", "value": 123.4, "date": "2025-09-01"},
        {"series": "UNRATE", "value": 4.0, "date": "2025-09-01"},
        {"series": "PPIACO", "value": 246.5, "date": "2025-10-01"},
        {"series": "PCEPI", "value": 123.5, "date": "2025-10-01"},
        {"series": "UNRATE", "value": 4.0, "date": "2025-10-01"},
        {"series": "PPIACO", "value": 246.6, "date": "2025-11-01"},
        {"series": "PCEPI", "value": 123.6, "date": "2025-11-01"},
        {"series": "UNRATE", "value": 4.0, "date": "2025-11-01"},
        {"series": "PPIACO", "value": 246.7, "date": "2025-12-01"},
        {"series": "PCEPI", "value": 123.7, "date": "2025-12-01"},
        {"series": "UNRATE", "value": 4.0, "date": "2025-12-01"},
    ]
    frame = build_training_frame_from_history(history, {"PPIACO": 246.7, "PCEPI": 123.7, "UNRATE": 4.0})
    assert len(frame) >= 11
    assert set(["ppi", "pcepi", "unrate", "cpi_mom_next"]).issubset(frame.columns)


def test_build_training_frame_with_cpiaucsl_target():
    """CPI m/m from FRED index levels; label is next-month % change."""
    from src.theses.economic_indicators import model

    cpi0, cpi1 = 300.0, 301.0
    history = [
        {"series": "PPIACO", "value": 240.0, "date": "2025-01-01"},
        {"series": "PCEPI", "value": 120.0, "date": "2025-01-01"},
        {"series": "UNRATE", "value": 4.0, "date": "2025-01-01"},
        {"series": model.CPI_SERIES_FRED, "value": cpi0, "date": "2025-01-01"},
        {"series": "PPIACO", "value": 241.0, "date": "2025-02-01"},
        {"series": "PCEPI", "value": 120.5, "date": "2025-02-01"},
        {"series": "UNRATE", "value": 4.0, "date": "2025-02-01"},
        {"series": model.CPI_SERIES_FRED, "value": cpi1, "date": "2025-02-01"},
    ]
    for m in range(3, 13):
        d = f"2025-{m:02d}-01"
        history.extend(
            [
                {"series": "PPIACO", "value": 240.0 + m, "date": d},
                {"series": "PCEPI", "value": 120.0 + m * 0.1, "date": d},
                {"series": "UNRATE", "value": 4.0, "date": d},
                {"series": model.CPI_SERIES_FRED, "value": cpi1 + m * 0.2, "date": d},
            ]
        )
    frame = build_training_frame_from_history(
        history, {"PPIACO": 250.0, "PCEPI": 125.0, "UNRATE": 4.0}
    )
    assert "cpi_mom_next" in frame.columns
    assert (frame["cpi_mom_next"].dropna() >= -1).all()  # no absurd negatives from levels


def test_mom_to_probability_monotonic():
    p_low = mom_percent_to_yes_probability(0.1, threshold_pct=0.3, scale=12.0)
    p_high = mom_percent_to_yes_probability(0.5, threshold_pct=0.3, scale=12.0)
    assert 0.01 <= p_low < p_high <= 0.99


def test_live_prediction_uses_latest_unlabeled_feature_row():
    """Next-month CPI forecast must use the newest feature month, not the last labeled row.

    Concrete trigger: history through Dec with CPI levels known through Dec.
    After shift(-1), Dec has no next-month label. Pre-fix code dropped Dec and
    predicted from Nov features (already-realized Dec m/m). Post-fix predicts
    from Dec features for the still-unknown Jan m/m.
    """
    from src.theses.economic_indicators import model

    history: list[dict] = []
    for m in range(1, 13):
        d = f"2025-{m:02d}-01"
        # Distinct Dec features so a lag would change the OLS input vector.
        ppi = 240.0 + m
        pce = 120.0 + m * 0.1
        unrate = 4.0 if m < 12 else 5.5
        cpi = 300.0 + m * 0.3
        history.extend(
            [
                {"series": "PPIACO", "value": ppi, "date": d},
                {"series": "PCEPI", "value": pce, "date": d},
                {"series": "UNRATE", "value": unrate, "date": d},
                {"series": model.CPI_SERIES_FRED, "value": cpi, "date": d},
            ]
        )

    frame = build_training_frame_from_history(
        history, {"PPIACO": 252.0, "PCEPI": 121.2, "UNRATE": 5.5}
    )
    assert frame["cpi_mom_next"].isna().iloc[-1]
    assert float(frame.iloc[-1]["unrate"]) == pytest.approx(5.5)
    assert float(frame.dropna(subset=["cpi_mom_next"]).iloc[-1]["unrate"]) == pytest.approx(4.0)

    result = train_validate_predict(frame)
    # Reconstruct coefficients on labeled rows and confirm prediction matches Dec features.
    import numpy as np

    labeled = frame.dropna(subset=["cpi_mom_next"]).sort_values("release_date")
    X = labeled[["ppi", "pcepi", "unrate"]].to_numpy(dtype=float)
    y = labeled["cpi_mom_next"].to_numpy(dtype=float)
    n = len(labeled)
    split_idx = max(8, int(n * 0.8))
    split_idx = min(split_idx, n - 1)
    coef, _, _, _ = np.linalg.lstsq(np.c_[np.ones(split_idx), X[:split_idx]], y[:split_idx], rcond=None)
    latest = frame.sort_values("release_date").iloc[-1][["ppi", "pcepi", "unrate"]].to_numpy(dtype=float)
    lagged = labeled.iloc[-1][["ppi", "pcepi", "unrate"]].to_numpy(dtype=float)
    expected_latest = float(np.r_[1.0, latest] @ coef)
    expected_lagged = float(np.r_[1.0, lagged] @ coef)
    assert result.prediction == pytest.approx(expected_latest)
    assert expected_latest != pytest.approx(expected_lagged)
