import duckdb
from pathlib import Path

from src.core.config import get_settings
from src.pipeline.run import run_pipeline


def test_pipeline_smoke_run():
    run_id, report_path = run_pipeline("economic_indicators")
    assert run_id
    assert report_path.exists()
    html_path = report_path.with_suffix(".html")
    assert html_path.exists()

    settings = get_settings()
    con = duckdb.connect(str(settings.duckdb_path))
    n = con.execute("SELECT COUNT(*) FROM model_forecasts WHERE run_id = ?", [run_id]).fetchone()[0]
    con.close()
    assert n >= 1

    artifacts_dir = Path(settings.data_dir) / "artifacts" / "economic_indicators" / run_id
    assert (artifacts_dir / "training_frame.csv").exists()
    assert (artifacts_dir / "forecast_summary.json").exists()
