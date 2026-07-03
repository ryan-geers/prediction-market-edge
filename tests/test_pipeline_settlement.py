from datetime import datetime, timezone

from src.core.schemas import PaperPositionRecord
from src.pipeline.run import _settlement_close_for_position


def test_void_settlement_refunds_entry_price() -> None:
    pos = PaperPositionRecord(
        position_id="pos-void",
        venue="kalshi",
        contract_id="CPI-MAY-OVER-0.3",
        net_qty=25.0,
        avg_entry_price=0.72,
        direction="yes",
    )
    closed_at = datetime(2026, 7, 3, tzinfo=timezone.utc)

    close = _settlement_close_for_position(pos, "void", closed_at)

    assert close.position_id == "pos-void"
    assert close.avg_exit_price == 0.72
    assert close.realized_pnl == 0.0
    assert close.close_reason == "contract_settled"
    assert close.closed_at_utc == closed_at


def test_no_position_settles_on_no_win() -> None:
    pos = PaperPositionRecord(
        position_id="pos-no",
        venue="kalshi",
        contract_id="CPI-MAY-OVER-0.3",
        net_qty=10.0,
        avg_entry_price=0.35,
        direction="no",
    )
    closed_at = datetime(2026, 7, 3, tzinfo=timezone.utc)

    close = _settlement_close_for_position(pos, "no", closed_at)

    assert close.avg_exit_price == 1.0
    assert close.realized_pnl == 6.5
