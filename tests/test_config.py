import pytest

from src.core.config import Settings


def test_settings_reads_edge_threshold_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EDGE_THRESHOLD_BPS", "450")
    s = Settings()
    assert s.edge_threshold_bps == 450


def test_settings_email_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EMAIL_DRY_RUN", "true")
    s = Settings()
    assert s.email_dry_run is True


def test_settings_default_edge_when_env_cleared(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EDGE_THRESHOLD_BPS", "300")
    s = Settings()
    assert s.edge_threshold_bps == 300
