from src.core.schemas import PaperPositionRecord
from src.pipeline.run import _settlement_exit_price


def _position(direction: str | None, avg_entry_price: float = 0.37) -> PaperPositionRecord:
    return PaperPositionRecord(
        venue="kalshi",
        contract_id="KXTEST",
        net_qty=10.0,
        avg_entry_price=avg_entry_price,
        direction=direction,
    )


def test_settlement_exit_price_is_direction_aware() -> None:
    assert _settlement_exit_price("yes", _position("yes")) == 1.0
    assert _settlement_exit_price("no", _position("yes")) == 0.0
    assert _settlement_exit_price("no", _position("no")) == 1.0
    assert _settlement_exit_price("yes", _position("no")) == 0.0


def test_void_settlement_refunds_entry_price() -> None:
    assert _settlement_exit_price("void", _position("yes", avg_entry_price=0.72)) == 0.72


def test_settlement_exit_price_skips_unknown_direction() -> None:
    assert _settlement_exit_price("yes", _position(None)) is None
