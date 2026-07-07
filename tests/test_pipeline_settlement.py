from src.core.schemas import PaperPositionRecord
from src.pipeline.run import _settlement_exit_price


def _position(direction: str, entry: float = 0.37) -> PaperPositionRecord:
    return PaperPositionRecord(
        venue="kalshi",
        contract_id="KXCPI-TEST",
        net_qty=10.0,
        avg_entry_price=entry,
        direction=direction,
    )


def test_settlement_exit_price_is_direction_aware():
    assert _settlement_exit_price(_position("yes"), "yes") == 1.0
    assert _settlement_exit_price(_position("yes"), "no") == 0.0
    assert _settlement_exit_price(_position("no"), "yes") == 0.0
    assert _settlement_exit_price(_position("no"), "no") == 1.0


def test_void_settlement_refunds_entry_price():
    assert _settlement_exit_price(_position("yes", entry=0.42), "void") == 0.42
    assert _settlement_exit_price(_position("no", entry=0.73), "void") == 0.73
