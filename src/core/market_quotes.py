"""
Conservative Kalshi-style YES quote handling.

Thin or one-sided books (e.g. bid=0, ask=1.0 → naive mid=0.50) inflate paper PnL
and trigger spurious signal-flip exits. This module centralises quote quality rules.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.core.config import Settings


@dataclass(frozen=True)
class YesQuoteAssessment:
    """Parsed YES-side quote with fair-value and executable marks."""

    best_bid: float
    best_ask: float
    fair_yes_mid: float | None
    spread_bps: float
    quality: str
    is_signal_quality: bool
    is_exit_quality: bool
    yes_bid_for_exit: float
    yes_ask_for_exit: float


def spread_bps(bid: float, ask: float) -> float:
    """Bid-ask spread in basis points relative to mid; inf when mid is zero."""
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return float("inf")
    return ((ask - bid) / mid) * 10000.0


def _clamp_prob(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


def assess_yes_quote(
    bid: float,
    ask: float,
    last_trade: float | None,
    settings: "Settings",
) -> YesQuoteAssessment:
    """
    Classify a YES quote and derive fair mid + executable exit marks.

    * **fair_yes_mid** — used for edge / signals; ``None`` → caller should hold.
    * **yes_bid_for_exit** — conservative mark/exit for long YES (what you can sell at).
    * **yes_ask_for_exit** — YES price stored for long NO rows (high ask ⇒ low NO value).
    """
    bid = _clamp_prob(bid)
    ask = _clamp_prob(ask)
    lt = _clamp_prob(last_trade) if last_trade is not None else None

    min_bid = float(settings.market_min_bid_for_quote)
    min_ask = float(settings.market_min_ask_for_quote)
    max_spread = float(settings.market_max_spread_bps)
    max_one_sided_ask = float(settings.market_max_one_sided_ask)

    sp = spread_bps(bid, ask) if ask > bid else float("inf")

    def _last_trade_mid() -> float | None:
        if lt is None:
            return None
        if lt <= min_bid or lt >= 1.0 - min_bid:
            return None
        return lt

    two_sided = (
        bid >= min_bid
        and ask >= min_ask
        and ask > bid
        and sp <= max_spread
    )
    if two_sided:
        mid = (bid + ask) / 2.0
        return YesQuoteAssessment(
            best_bid=bid,
            best_ask=ask,
            fair_yes_mid=mid,
            spread_bps=sp,
            quality="two_sided",
            is_signal_quality=True,
            is_exit_quality=True,
            yes_bid_for_exit=bid,
            yes_ask_for_exit=ask,
        )

    # Empty or absurd one-sided book (bid=0, ask≈1.0) — the May-22 phantom-PnL pattern.
    if bid < min_bid and ask > max_one_sided_ask:
        return YesQuoteAssessment(
            best_bid=bid,
            best_ask=ask,
            fair_yes_mid=None,
            spread_bps=sp,
            quality="unusable_empty_book",
            is_signal_quality=False,
            is_exit_quality=False,
            yes_bid_for_exit=0.0,
            yes_ask_for_exit=ask,
        )

    # One-sided with a plausible ask (bid missing but ask not pinned at $1).
    if bid < min_bid and min_ask < ask <= max_one_sided_ask:
        lt_mid = _last_trade_mid()
        if lt_mid is not None:
            return YesQuoteAssessment(
                best_bid=bid,
                best_ask=ask,
                fair_yes_mid=lt_mid,
                spread_bps=sp,
                quality="last_trade_one_sided",
                is_signal_quality=True,
                is_exit_quality=False,
                yes_bid_for_exit=0.0,
                yes_ask_for_exit=ask,
            )
        # Allow signals vs ask proxy; still no exit (no bid to sell into).
        return YesQuoteAssessment(
            best_bid=bid,
            best_ask=ask,
            fair_yes_mid=ask,
            spread_bps=sp,
            quality="one_sided_ask_proxy",
            is_signal_quality=True,
            is_exit_quality=False,
            yes_bid_for_exit=0.0,
            yes_ask_for_exit=ask,
        )

    # Two prices present but spread too wide. A last trade has no freshness signal
    # here, so do not let it override an untradeable current book.
    if bid >= min_bid and ask > bid and sp > max_spread:
        return YesQuoteAssessment(
            best_bid=bid,
            best_ask=ask,
            fair_yes_mid=None,
            spread_bps=sp,
            quality="unusable_wide_spread",
            is_signal_quality=False,
            is_exit_quality=False,
            yes_bid_for_exit=bid if bid >= min_bid else 0.0,
            yes_ask_for_exit=ask,
        )

    lt_mid = _last_trade_mid()
    if lt_mid is not None:
        exit_ok = bid >= min_bid
        return YesQuoteAssessment(
            best_bid=bid,
            best_ask=ask,
            fair_yes_mid=lt_mid,
            spread_bps=sp,
            quality="last_trade_fallback",
            is_signal_quality=True,
            is_exit_quality=exit_ok,
            yes_bid_for_exit=bid if exit_ok else 0.0,
            yes_ask_for_exit=ask,
        )

    return YesQuoteAssessment(
        best_bid=bid,
        best_ask=ask,
        fair_yes_mid=None,
        spread_bps=sp,
        quality="unusable",
        is_signal_quality=False,
        is_exit_quality=False,
        yes_bid_for_exit=bid if bid >= min_bid else 0.0,
        yes_ask_for_exit=ask,
    )


def mark_yes_for_direction(assessment: YesQuoteAssessment, direction: str | None) -> float | None:
    """
    Direction-aware YES mark stored in ``paper_positions.mark_price``.

    Long YES → bid (sellable). Long NO → ask (high YES ask ⇒ low NO mark via 1 - mark in PnL).
    Returns ``None`` when the quote is too broken to re-mark (caller keeps prior mark).
    """
    if direction == "yes":
        if not assessment.is_exit_quality or assessment.yes_bid_for_exit <= 0:
            return None
        return assessment.yes_bid_for_exit
    if direction == "no":
        if not assessment.is_exit_quality:
            return None
        return assessment.yes_ask_for_exit
    return assessment.fair_yes_mid


def executable_yes_exit_price(
    assessment: YesQuoteAssessment,
    direction: str,
) -> float | None:
    """Price for closing a long YES or long NO position; ``None`` if not tradable."""
    if direction == "yes":
        if not assessment.is_exit_quality or assessment.yes_bid_for_exit <= 0:
            return None
        return assessment.yes_bid_for_exit
    if direction == "no":
        # NO exit mark in position price space: 1 - yes_ask
        if not assessment.is_exit_quality:
            return None
        return 1.0 - assessment.yes_ask_for_exit
    return None
