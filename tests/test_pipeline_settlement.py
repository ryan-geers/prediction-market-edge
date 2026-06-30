from src.pipeline.run import _settlement_exit_price


def test_settlement_exit_price_for_yes_and_no_directions() -> None:
    assert _settlement_exit_price("yes", "yes", 0.40) == 1.0
    assert _settlement_exit_price("no", "yes", 0.40) == 0.0
    assert _settlement_exit_price("no", "no", 0.40) == 1.0
    assert _settlement_exit_price("yes", "no", 0.40) == 0.0


def test_void_settlement_refunds_entry_price() -> None:
    assert _settlement_exit_price("void", "yes", 0.23) == 0.23
    assert _settlement_exit_price("void", "no", 0.77) == 0.77
