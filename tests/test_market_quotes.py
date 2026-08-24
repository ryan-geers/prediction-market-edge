"""Quote sanity rules — prevent (bid=0, ask=1) phantom mids."""

from src.core.config import Settings
from src.core.market_quotes import (
    assess_yes_quote,
    executable_yes_exit_price,
)


def _settings(market_max_spread_bps_hard: float = 5_000.0) -> Settings:
    return Settings(
        market_min_bid_for_quote=0.01,
        market_min_ask_for_quote=0.01,
        market_max_spread_bps=1500.0,
        market_max_one_sided_ask=0.85,
        market_max_spread_bps_hard=market_max_spread_bps_hard,
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


def test_one_sided_low_ask_blocked_by_hard_cap():
    """Default hard-cap (5,000 bps) blocks one-sided books whose effective
    spread is 20,000 bps — no exit liquidity, so entering would trap capital."""
    qa = assess_yes_quote(0.0, 0.10, None, _settings())
    assert qa.quality == "unusable_one_sided_hard_cap"
    assert not qa.is_signal_quality
    assert not qa.is_exit_quality
    assert executable_yes_exit_price(qa, "yes") is None


def test_one_sided_low_ask_allows_signal_when_hard_cap_relaxed():
    """When market_max_spread_bps_hard is raised above 20,000 bps, one-sided
    books with a plausible ask are still allowed as signals (ask proxy)."""
    qa = assess_yes_quote(0.0, 0.10, None, _settings(market_max_spread_bps_hard=25_000.0))
    assert qa.quality == "one_sided_ask_proxy"
    assert qa.fair_yes_mid == 0.10
    assert qa.is_signal_quality
    assert not qa.is_exit_quality
    assert executable_yes_exit_price(qa, "yes") is None


def test_last_trade_blocked_when_one_sided_hard_cap():
    """last_trade_one_sided quality is also blocked by the hard cap."""
    qa = assess_yes_quote(0.0, 0.10, 0.06, _settings())
    assert qa.quality == "unusable_one_sided_hard_cap"
    assert not qa.is_signal_quality
    # fair_yes_mid is still populated from last_trade for marking existing positions.
    assert qa.fair_yes_mid == 0.06


def test_last_trade_one_sided_allowed_when_hard_cap_relaxed():
    """last_trade_one_sided quality is preserved when the hard cap permits it."""
    qa = assess_yes_quote(0.0, 0.10, 0.06, _settings(market_max_spread_bps_hard=25_000.0))
    assert qa.quality == "last_trade_one_sided"
    assert qa.fair_yes_mid == 0.06
    assert not qa.is_exit_quality


def test_executable_yes_exit_uses_bid():
    qa = assess_yes_quote(0.48, 0.52, None, _settings())
    assert executable_yes_exit_price(qa, "yes") == 0.48
    assert executable_yes_exit_price(qa, "no") == 0.48  # 1 - 0.52


def test_stale_last_trade_outside_live_book_is_not_fair_mid():
    """Production KXU3-26SEP-T4.3: last_trade=0.24 with bid=0.31/ask=0.37.

    Spread is 1765 bps (between 1500 and 5000), so the wide-spread last-trade
    rescue used to treat 0.24 as fair mid → enter YES. The next run's 1¢ bid
    tick made the same book two-sided at mid 0.345 → flip to NO.
    """
    qa = assess_yes_quote(0.31, 0.37, 0.24, _settings())
    assert qa.quality == "unusable_wide_spread"
    assert qa.fair_yes_mid is None
    assert not qa.is_signal_quality
    assert not qa.is_exit_quality
    qa_high = assess_yes_quote(0.31, 0.37, 0.50, _settings())
    assert qa_high.quality == "unusable_wide_spread"
    assert qa_high.fair_yes_mid is None


def test_last_trade_inside_wide_spread_still_usable():
    """A last print that sits inside the live bid/ask is still a valid mid
    when the spread is wide but under the hard cap."""
    qa = assess_yes_quote(0.31, 0.37, 0.34, _settings())
    assert qa.quality == "last_trade_wide_spread"
    assert qa.fair_yes_mid == 0.34
    assert qa.is_signal_quality
    assert qa.is_exit_quality


def test_last_trade_at_bid_or_ask_is_inside_book():
    qa_bid = assess_yes_quote(0.31, 0.37, 0.31, _settings())
    assert qa_bid.quality == "last_trade_wide_spread"
    assert qa_bid.fair_yes_mid == 0.31
    qa_ask = assess_yes_quote(0.31, 0.37, 0.37, _settings())
    assert qa_ask.quality == "last_trade_wide_spread"
    assert qa_ask.fair_yes_mid == 0.37
