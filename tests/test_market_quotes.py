"""Quote sanity rules — prevent (bid=0, ask=1) phantom mids."""

from src.core.config import Settings
from src.core.market_quotes import (
    assess_yes_quote,
    executable_yes_exit_price,
)


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
    assert executable_yes_exit_price(qa, "no") is None


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


def test_wide_spread_last_trade_is_not_signal_or_exit_quality():
    qa = assess_yes_quote(0.02, 0.99, 0.50, _settings())
    assert qa.quality == "unusable_wide_spread"
    assert qa.fair_yes_mid is None
    assert not qa.is_signal_quality
    assert not qa.is_exit_quality
    assert executable_yes_exit_price(qa, "yes") is None
    assert executable_yes_exit_price(qa, "no") is None
