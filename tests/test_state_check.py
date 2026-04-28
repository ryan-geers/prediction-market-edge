from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.core.schemas import RunManifest
from src.core.storage import Storage
from src.pipeline.state_check import check_state_freshness


def test_check_state_missing_db(tmp_path: Path) -> None:
    db = tmp_path / "missing.duckdb"
    assert check_state_freshness(db, max_run_age_days=7) == 1


def test_check_state_empty_manifest(tmp_path: Path) -> None:
    db = tmp_path / "e.duckdb"
    Storage(db).close()
    assert check_state_freshness(db, max_run_age_days=7) == 1


def test_check_state_stale_run(tmp_path: Path) -> None:
    db = tmp_path / "s.duckdb"
    st = Storage(db)
    old = datetime.now(timezone.utc) - timedelta(days=30)
    st.upsert_run_manifest(
        RunManifest(
            run_id="r1",
            code_commit_sha="a",
            config_hash="b",
            active_thesis="economic_indicators",
            data_sources="fred",
            started_at_utc=old,
            completed_at_utc=old,
        )
    )
    st.close()
    assert check_state_freshness(db, max_run_age_days=7) == 1


def test_check_state_fresh(tmp_path: Path) -> None:
    db = tmp_path / "f.duckdb"
    st = Storage(db)
    recent = datetime.now(timezone.utc) - timedelta(hours=1)
    st.upsert_run_manifest(
        RunManifest(
            run_id="r2",
            code_commit_sha="a",
            config_hash="b",
            active_thesis="economic_indicators",
            data_sources="fred",
            started_at_utc=recent,
            completed_at_utc=recent,
        )
    )
    st.close()
    assert check_state_freshness(db, max_run_age_days=7) == 0
