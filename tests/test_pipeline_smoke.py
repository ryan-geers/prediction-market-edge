import duckdb

from src.core.config import get_settings
from src.pipeline.run import run_pipeline


def test_pipeline_smoke_run():
    """Verifies the full pipeline executes and writes core data to DuckDB.
    Artifact file I/O (run reports, training_frame, forecast_summary) is gated
    by SAVE_RUN_ARTIFACTS=true and is not checked here — set that env var to
    test artifact generation explicitly."""
    run_id, report_path = run_pipeline("economic_indicators")
    assert run_id
    # report_path is None when save_run_artifacts=False (the default for local runs)
    assert report_path is None or report_path.exists()

    settings = get_settings()
    con = duckdb.connect(str(settings.duckdb_path))
    n = con.execute("SELECT COUNT(*) FROM model_forecasts WHERE run_id = ?", [run_id]).fetchone()[0]
    con.close()
    assert n >= 1
