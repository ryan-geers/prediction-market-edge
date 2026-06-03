"""Quote sanity rules — prevent (bid=0, ask=1) phantom mids."""

from src.core.config import Settings
from src.core.market_quotes import (
    assess_yes_quote,
    executable_yes_exit_price,
)
from src.theses.economic_indicators.module import EconomicIndicatorsThesis


def _settings() -> Settings:
    return Settings(
        market_min_bid_for_quote=0.01,
        market_min_ask_for_quote=0.01,
        market_max_spread_bps=1500.0,
        market_max_one_sided_ask=0.85,
    )


def test_two_sided_quote_uses_mid():
    qa = assess_yes_quote(0.49, 0.51, None, _settings())
    assert qa.quality == "two_sided"
    assert qa.fair_yes_mid == 0.5
    assert qa.is_signal_quality
    assert qa.is_exit_quality


def test_empty_book_bid_zero_ask_one_is_unusable():
    """May-22 pattern: bid=0, ask=1.0 must not produce fair mid 0.50."""
    qa = assess_yes_quote(0.0, 1.0, 0.0, _settings())
    assert qa.quality == "unusable_empty_book"
    assert qa.fair_yes_mid is None
    assert not qa.is_signal_quality
    assert not qa.is_exit_quality
    assert executable_yes_exit_price(qa, "yes") is None


def test_one_sided_low_ask_allows_signal_but_not_exit():
    qa = assess_yes_quote(0.0, 0.10, None, _settings())
    assert qa.quality == "one_sided_ask_proxy"
    assert qa.fair_yes_mid == 0.10
    assert qa.is_signal_quality
    assert not qa.is_exit_quality
    assert executable_yes_exit_price(qa, "yes") is None


def test_last_trade_used_when_one_sided():
    qa = assess_yes_quote(0.0, 0.10, 0.06, _settings())
    assert qa.quality == "last_trade_one_sided"
    assert qa.fair_yes_mid == 0.06
    assert not qa.is_exit_quality


def test_executable_yes_exit_uses_bid():
    qa = assess_yes_quote(0.48, 0.52, None, _settings())
    assert executable_yes_exit_price(qa, "yes") == 0.48
    assert executable_yes_exit_price(qa, "no") == 0.48  # 1 - 0.52


def test_one_sided_ask_does_not_generate_long_no_signal():
    thesis = EconomicIndicatorsThesis(_settings())
    forecast = {
        "model_healthy": True,
        "un_healthy": False,
        "un_reg": None,
        "market": [
            {
                "venue": "kalshi",
                "contract_id": "KXCPI-26MAY-T0.3",
                "label": "CPI over 0.3%",
                "best_bid": 0.0,
                "best_ask": 0.10,
                "last_trade": None,
                "series_ticker": "KXCPI",
                "contract_type": "cpi",
                "threshold": 0.3,
            }
        ],
        "model_probability": 0.05,
        "predicted_cpi_mom_pct": 0.0,
        "validation_rmse": 0.1,
        "walk_forward_val_rmse": 0.1,
        "macro_history_count": 24,
    }

    signals, _ = thesis.generate_signals("run-test", forecast)

    assert len(signals) == 1
    assert signals[0].decision == "hold"
    assert "quote_no_bid_for_no_entry=true" in signals[0].decision_reason
