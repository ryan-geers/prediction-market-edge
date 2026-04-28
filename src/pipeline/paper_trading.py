"""
Paper-trade simulation: map signal decisions to filled orders and position rows
with slippage, fee assumptions, and mark-to-market (optional same-run EOD close).
"""
from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

from src.core.schemas import PaperOrderRecord, PaperPositionRecord, SignalRecord, utc_now

if TYPE_CHECKING:
    from src.core.config import Settings


def _apply_slippage_to_yes_ask(ask: float, slippage_bps: float) -> float:
    adj = ask * (1.0 + slippage_bps / 10000.0)
    return min(0.999, max(0.001, adj))


def _apply_slippage_to_no_ask(yes_bid: float, slippage_bps: float) -> float:
    """Cost to lift NO (1 - yes_bid) with slippage on that price."""
    no_ask = 1.0 - yes_bid
    adj = no_ask * (1.0 + slippage_bps / 10000.0)
    return min(0.999, max(0.001, adj))


def _no_mark_value(yes_mid: float) -> float:
    return 1.0 - yes_mid


def _position_qty(fill_price: float, settings: "Settings") -> float:
    """
    Derive contract quantity from bankroll and position-size percentage.
    budget = bankroll * position_size_pct (e.g. $500 * 5% = $25)
    qty    = budget / fill_price          (e.g. $25 / $0.50 = 50 contracts)
    Falls back to paper_default_qty if fill_price is zero or either setting is unset.
    """
    try:
        budget = float(settings.paper_bankroll) * float(settings.paper_position_size_pct)
        if fill_price > 0 and budget > 0:
            return budget / fill_price
    except (TypeError, ValueError, ZeroDivisionError):
        pass
    return float(settings.paper_default_qty)


def simulate_paper_trades(
    signals: list[SignalRecord],
    settings: Settings,
) -> tuple[list[PaperOrderRecord], list[PaperPositionRecord]]:
    """
    Create paper orders/positions for actionable signals. Skips hold/reject.
    Unrealized PnL is per contract in price space (0–1) after entry and fees.
    If ``paper_eod_close`` is True, closes each new position at the same mark (realized PnL, demo / stress mode).
    """
    orders: list[PaperOrderRecord] = []
    positions: list[PaperPositionRecord] = []
    now = utc_now()
    slippage = float(settings.paper_slippage_bps)
    fees = float(settings.paper_fees_assumption_bps)
    assumption = settings.paper_assumption_version
    fill_rule = settings.paper_fill_rule
    slippage_model = settings.paper_slippage_model_name
    eod = settings.paper_eod_close
    yes_mid = 0.0  # set per signal

    for signal in signals:
        if signal.decision not in {"enter_long_yes", "enter_long_no"}:
            continue

        side: str = "yes" if signal.decision == "enter_long_yes" else "no"
        yes_mid = float(signal.market_implied_probability)
        if side == "yes":
            raw_fill = _apply_slippage_to_yes_ask(signal.ask_price, slippage)
        else:
            raw_fill = _apply_slippage_to_no_ask(signal.bid_price, slippage)

        qty = _position_qty(raw_fill, settings)

        fee_paid = raw_fill * (fees / 10000.0) * qty
        effective_entry = raw_fill
        no_mid = _no_mark_value(yes_mid)

        if side == "yes":
            gross_mtm = (yes_mid - effective_entry) * qty
        else:
            gross_mtm = (no_mid - effective_entry) * qty
        net_unreal = gross_mtm - fee_paid

        order = PaperOrderRecord(
            signal_id=signal.signal_id,
            run_id=signal.run_id,
            venue=signal.venue,
            contract_id=signal.contract_id,
            side=side,  # type: ignore[arg-type]
            order_type="limit",
            qty=qty,
            limit_price=raw_fill,
            fill_price=raw_fill,
            fill_qty=qty,
            status="filled",
            submitted_at_utc=now,
            filled_at_utc=now,
            fill_rule=fill_rule,
            slippage_assumption_bps=slippage,
            fees_assumption_bps=fees,
            assumption_version=assumption,
            slippage_model_name=slippage_model,
        )
        orders.append(order)

        if eod:
            exit_px = yes_mid if side == "yes" else no_mid
            realized = (exit_px - effective_entry) * qty - fee_paid
            positions.append(
                PaperPositionRecord(
                    position_id=str(uuid4()),
                    run_id=signal.run_id,
                    signal_id=signal.signal_id,
                    venue=signal.venue,
                    contract_id=signal.contract_id,
                    opened_at_utc=now,
                    closed_at_utc=now,
                    net_qty=0.0,
                    avg_entry_price=effective_entry,
                    avg_exit_price=exit_px,
                    realized_pnl=realized,
                    unrealized_pnl=0.0,
                    mark_price=exit_px,
                    last_mark_time_utc=now,
                    status="closed",
                    close_reason="eod_mark",
                )
            )
        else:
            mark_px = yes_mid if side == "yes" else no_mid
            positions.append(
                PaperPositionRecord(
                    position_id=str(uuid4()),
                    run_id=signal.run_id,
                    signal_id=signal.signal_id,
                    venue=signal.venue,
                    contract_id=signal.contract_id,
                    opened_at_utc=now,
                    closed_at_utc=None,
                    net_qty=qty,
                    avg_entry_price=effective_entry,
                    realized_pnl=0.0,
                    unrealized_pnl=net_unreal,
                    mark_price=mark_px,
                    last_mark_time_utc=now,
                    status="open",
                )
            )

    return orders, positions
