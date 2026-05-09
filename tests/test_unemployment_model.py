"""Tests for the unemployment AR model."""

import pytest

from src.connectors.kalshi import _parse_threshold
from src.theses.economic_indicators.unemployment_model import (
    build_unemployment_training_frame,
    train_validate_predict_unemployment,
    unrate_to_yes_probability,
)


def _make_history(n_months: int = 24, base: float = 4.2) -> list[dict]:
    """Synthetic monthly UNRATE history rows."""
    rows = []
    for i in range(n_months):
        year = 2024 + (i // 12)
        month = (i % 12) + 1
        rows.append(
            {
                "series": "UNRATE",
                "value": round(base + 0.02 * (i % 6), 2),
                "date": f"{year}-{month:02d}-01",
            }
        )
    return rows


# --- build_unemployment_training_frame ---

def test_frame_uses_real_history():
    history = _make_history(30)
    df = build_unemployment_training_frame(history)
    assert len(df) > 0
    assert "unrate_next" in df.columns
    assert "unrate_t" in df.columns
    assert "unrate_lag1" in df.columns
    assert "unrate_lag3" in df.columns
    assert "trend_3m" in df.columns


def test_frame_falls_back_to_synthetic_on_empty_history():
    df = build_unemployment_training_frame([])
    assert len(df) > 0
    assert "unrate_next" in df.columns


def test_frame_falls_back_on_insufficient_history():
    # Only 5 months — below MIN_TRAIN_ROWS + 4 threshold.
    history = _make_history(5)
    df = build_unemployment_training_frame(history, fallback_unrate=4.5)
    assert len(df) > 0
    # Synthetic frame centres around the fallback value.
    assert df["unrate_t"].mean() == pytest.approx(4.5, abs=0.5)


def test_frame_accepts_lns_series_name():
    """LNS14000000 (BLS series name) should be treated as UNRATE."""
    history = [
        {"series": "LNS14000000", "value": 4.1 + i * 0.01, "date": f"2024-{i+1:02d}-01"}
        for i in range(20)
    ]
    df = build_unemployment_training_frame(history)
    assert len(df) > 0


# --- train_validate_predict_unemployment ---

def test_model_produces_sensible_prediction():
    history = _make_history(48, base=4.2)
    df = build_unemployment_training_frame(history)
    result = train_validate_predict_unemployment(df)
    assert 1.0 <= result.prediction <= 15.0
    assert result.rmse >= 0
    assert result.n_train > 0
    assert result.n_val > 0
    assert result.training_start < result.training_end


def test_model_healthy_flag_set_on_valid_data():
    history = _make_history(48)
    df = build_unemployment_training_frame(history)
    result = train_validate_predict_unemployment(df)
    assert result.model_healthy is True


def test_model_raises_on_too_few_rows():
    history = _make_history(5)
    df = build_unemployment_training_frame(history)
    # Synthetic fallback has enough rows, but if we slice it down forcefully:
    import pandas as pd
    tiny = df.head(5)
    with pytest.raises(ValueError, match="Insufficient rows"):
        train_validate_predict_unemployment(tiny)


def test_walk_forward_populated():
    history = _make_history(60)
    df = build_unemployment_training_frame(history)
    result = train_validate_predict_unemployment(df)
    assert result.walk_forward_val_rmse >= 0
    assert result.backtest["walk_forward_steps"] > 0


# --- unrate_to_yes_probability ---

def test_probability_above_threshold_high():
    # If predicted rate (5.0) is well above threshold (4.0), P(YES) should be high.
    p = unrate_to_yes_probability(5.0, threshold=4.0, scale=30.0)
    assert p > 0.95


def test_probability_below_threshold_low():
    p = unrate_to_yes_probability(3.5, threshold=4.5, scale=30.0)
    assert p < 0.05


def test_probability_at_threshold_near_half():
    p = unrate_to_yes_probability(4.2, threshold=4.2, scale=30.0)
    assert abs(p - 0.5) < 0.01


def test_probability_bounded():
    for pred in [-10, 0, 4.2, 20, 100]:
        p = unrate_to_yes_probability(pred, threshold=4.2)
        assert 0.01 <= p <= 0.99


def test_probability_monotonic():
    thresholds = [3.5, 4.0, 4.2, 4.5, 5.0]
    probs = [unrate_to_yes_probability(4.3, threshold=t) for t in thresholds]
    # Higher threshold → lower P(YES) for the same prediction.
    assert probs == sorted(probs, reverse=True)


# --- _parse_threshold ---

def test_parse_threshold_kxu3():
    assert _parse_threshold("KXU3-26MAY-T4.8") == pytest.approx(4.8)


def test_parse_threshold_kxeconstatu3():
    assert _parse_threshold("KXECONSTATU3-26NOV-T5.5") == pytest.approx(5.5)


def test_parse_threshold_over_format():
    # OVER-{value} format used by CPI contracts is now parsed correctly.
    assert _parse_threshold("CPI-MAY-OVER-0.3") == pytest.approx(0.3)


def test_parse_threshold_integer():
    assert _parse_threshold("KXU3-26JUN-T4") == pytest.approx(4.0)
