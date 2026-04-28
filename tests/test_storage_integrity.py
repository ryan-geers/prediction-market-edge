from pathlib import Path

import duckdb

from src.core.schemas import RunManifest, utc_now
from src.core.storage import SCHEMA_SQL, SCHEMA_MIGRATIONS, Storage


EXPECTED_TABLES = {
    "signals",
    "paper_orders",
    "paper_positions",
    "market_snapshots",
    "run_manifest",
    "model_forecasts",
}

SIGNAL_COLUMNS = {
    "signal_id",
    "run_id",
    "thesis_module",
    "venue",
    "contract_id",
    "model_probability",
    "decision",
    "assumption_version",
}


def test_duckdb_bootstrap_creates_required_tables(tmp_path: Path) -> None:
    db = tmp_path / "t.duckdb"
    _ = Storage(db)
    con = duckdb.connect(str(db))
    try:
        rows = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
        names = {r[0] for r in rows}
        assert EXPECTED_TABLES <= names
        col_names = {r[1] for r in con.execute("PRAGMA table_info('signals')").fetchall()}
        assert SIGNAL_COLUMNS <= col_names
    finally:
        con.close()


def test_migrations_idempotent(tmp_path: Path) -> None:
    db = tmp_path / "m.duckdb"
    con = duckdb.connect(str(db))
    try:
        con.execute(SCHEMA_SQL)
        for stmt in SCHEMA_MIGRATIONS:
            con.execute(stmt)
        for stmt in SCHEMA_MIGRATIONS:
            con.execute(stmt)
    finally:
        con.close()
    st = Storage(db)
    st.close()
    con2 = duckdb.connect(str(db))
    try:
        n = con2.execute("SELECT COUNT(*) FROM paper_positions").fetchone()[0]
        assert n == 0
    finally:
        con2.close()


def test_run_manifest_roundtrip(tmp_path: Path) -> None:
    db = tmp_path / "r.duckdb"
    st = Storage(db)
    m = RunManifest(
        run_id="rid",
        code_commit_sha="abc",
        config_hash="def",
        active_thesis="economic_indicators",
        data_sources="fred",
        started_at_utc=utc_now(),
        completed_at_utc=utc_now(),
    )
    st.upsert_run_manifest(m)
    st.close()
    con = duckdb.connect(str(db))
    try:
        one = con.execute("SELECT run_id FROM run_manifest WHERE run_id = ?", ["rid"]).fetchone()
        assert one is not None
    finally:
        con.close()
