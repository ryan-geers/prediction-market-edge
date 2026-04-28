"""
Partitioned Parquet snapshots for key DuckDB tables (v1 archival export).

Intended for manual or scheduled export after pipeline runs; not required for core execution.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import duckdb

# Tables aligned with storage/bootstrap schema; do not accept arbitrary names from callers.
_ARCHIVE_TABLES = (
    "signals",
    "paper_orders",
    "paper_positions",
    "market_snapshots",
    "run_manifest",
    "model_forecasts",
)


def export_tables_to_parquet_partition(
    duckdb_path: Path,
    archive_root: Path,
    partition_date: date | None = None,
    *,
    tables: tuple[str, ...] = _ARCHIVE_TABLES,
) -> list[Path]:
    """
    Write each table to ``archive_root/dt=YYYY-MM-DD/<table>.parquet``.

    Returns paths created (including zero-byte exports for empty tables if DuckDB allows).
    """
    if partition_date is None:
        partition_date = datetime.now(timezone.utc).date()
    dt_dir = archive_root / f"dt={partition_date.isoformat()}"
    dt_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(duckdb_path), read_only=True)
    written: list[Path] = []
    try:
        for table in tables:
            if table not in _ARCHIVE_TABLES:
                raise ValueError(f"Unknown table: {table}")
            out = dt_dir / f"{table}.parquet"
            con.execute(f'COPY (SELECT * FROM "{table}") TO ? (FORMAT PARQUET)', [str(out)])
            written.append(out)
    finally:
        con.close()
    return written


def validate_parquet_readable(parquet_path: Path) -> int:
    """Return row count from reading the Parquet file (sanity check)."""
    con = duckdb.connect()
    try:
        n = con.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(parquet_path)]).fetchone()[0]
        return int(n)
    finally:
        con.close()
