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
