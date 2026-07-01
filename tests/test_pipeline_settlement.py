from datetime import datetime, timezone

from src.core.schemas import PaperPositionRecord
from src.pipeline.run import _position_close_for_settlement


def _position(direction: str, avg_entry_price: float = 0.72) -> PaperPositionRecord:
    return PaperPositionRecord(
        position_id=f"pos-{direction}",
        venue="kalshi",
        contract_id="KXCPI-TEST",
        net_qty=10.0,
        avg_entry_price=avg_entry_price,
        unrealized_pnl=0.0,
        status="open",
        direction=direction,  # type: ignore[arg-type]
    )


def test_settlement_close_pays_directional_winner() -> None:
    closed_at = datetime(2026, 7, 1, tzinfo=timezone.utc)

    yes_close = _position_close_for_settlement(_position("yes", 0.40), "yes", closed_at)
    no_close = _position_close_for_settlement(_position("no", 0.35), "yes", closed_at)

    assert yes_close is not None
    assert yes_close.avg_exit_price == 1.0
    assert abs(yes_close.realized_pnl - 6.0) < 1e-9
    assert no_close is not None
    assert no_close.avg_exit_price == 0.0
    assert abs(no_close.realized_pnl - (-3.5)) < 1e-9


def test_settlement_close_void_refunds_entry_price() -> None:
    closed_at = datetime(2026, 7, 1, tzinfo=timezone.utc)

    close = _position_close_for_settlement(_position("yes", 0.72), "void", closed_at)

    assert close is not None
    assert close.avg_exit_price == 0.72
    assert close.realized_pnl == 0.0
