from pathlib import Path

from src.core.storage import Storage
from src.pipeline.parquet_archive import export_tables_to_parquet_partition, validate_parquet_readable


def test_parquet_export_reads_back(tmp_path: Path) -> None:
    db = tmp_path / "p.duckdb"
    Storage(db).close()
    archive = tmp_path / "archive"
    paths = export_tables_to_parquet_partition(db, archive)
    assert len(paths) == 6
    for p in paths:
        assert p.is_file()
        validate_parquet_readable(p)
