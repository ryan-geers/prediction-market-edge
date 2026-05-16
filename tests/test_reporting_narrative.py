"""Sanity checks for weekly-digest / position plain-English copy."""

from src.pipeline.reporting import _position_rationale


def test_unemployment_yes_narrative_when_point_below_threshold() -> None:
    """Long YES while the point forecast is below strike — must not say 'above the threshold'."""
    reason = (
        "contract_type=unemployment;model_vs_mid_edge_bps=2600;"
        "pred_unrate=4.328;threshold=4.6;val_rmse=0.89"
    )
    out = _position_rationale(
        "enter_long_yes",
        0.286,
        0.025,
        2600.0,
        reason,
    )
    assert "above the 4.6" not in out.lower()
    assert "below the 4.6" in out.lower() or "below the 4.6%" in out.lower()
    assert "tail" in out.lower() or "underpriced" in out.lower()


def test_unemployment_yes_narrative_when_point_above_threshold() -> None:
    reason = (
        "contract_type=unemployment;model_vs_mid_edge_bps=500;"
        "pred_unrate=5.1;threshold=4.6;val_rmse=0.89"
    )
    out = _position_rationale(
        "enter_long_yes",
        0.55,
        0.40,
        1500.0,
        reason,
    )
    assert "above the 4.6" in out.lower()
