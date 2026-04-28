import hashlib
import json
import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from src.core.config import get_settings
from src.core.logging import setup_logging
from src.core.schemas import RunManifest
from src.core.storage import Storage
from src.pipeline.reporting import generate_run_report, generate_run_report_html
from src.theses.registry import build_registry

LOGGER = logging.getLogger(__name__)


def _git_sha() -> str:
    try:
        return (
            subprocess.check_output(["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL)
            .decode()
            .strip()
        )
    except Exception:
        return "unknown"


def _config_hash(settings_dict: dict) -> str:
    raw = json.dumps(settings_dict, sort_keys=True, default=str).encode()
    return hashlib.sha256(raw).hexdigest()[:16]


def run_pipeline(thesis_name: str = "economic_indicators") -> tuple[str, Path]:
    settings = get_settings()
    setup_logging(settings.log_level)
    storage = Storage(settings.duckdb_path)
    run_id = str(uuid4())
    started = datetime.now(timezone.utc)

    manifest = RunManifest(
        run_id=run_id,
        code_commit_sha=_git_sha(),
        config_hash=_config_hash(settings.model_dump()),
        active_thesis=thesis_name,
        data_sources="fred,bls,bea,kalshi,polymarket",
        started_at_utc=started,
    )
    storage.upsert_run_manifest(manifest)

    registry = build_registry(settings)
    thesis = registry[thesis_name]
    raw = thesis.ingest()
    features = thesis.build_features(raw)
    forecast = thesis.forecast(features)
    forecast_records = thesis.build_forecast_records(run_id, forecast)
    signals, snapshots = thesis.generate_signals(run_id, forecast)
    orders, positions = thesis.paper_trade(signals)

    storage.insert_model_forecasts(forecast_records)
    storage.insert_signals(signals)
    storage.insert_snapshots(snapshots)
    storage.insert_orders(orders)
    storage.insert_positions(positions)

    manifest.completed_at_utc = datetime.now(timezone.utc)
    storage.upsert_run_manifest(manifest)
    storage.close()

    report_text = generate_run_report(str(settings.duckdb_path), run_id)
    reports_dir = settings.data_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    report_path = reports_dir / f"run_report_{run_id}.md"
    report_path.write_text(report_text)
    report_html_path = reports_dir / f"run_report_{run_id}.html"
    report_html_path.write_text(generate_run_report_html(str(settings.duckdb_path), run_id))

    # Persist lightweight model artifacts for auditability by release date/run.
    artifacts_dir = settings.data_dir / "artifacts" / thesis_name / run_id
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    if "training_df" in features:
        features["training_df"].to_csv(artifacts_dir / "training_frame.csv", index=False)
    artifact_payload = {
        "run_id": run_id,
        "thesis": thesis_name,
        "model_probability": forecast.get("model_probability"),
        "predicted_cpi_mom_pct": forecast.get("predicted_cpi_mom_pct"),
        "release_date": str(forecast.get("release_date")),
        "target_metric": forecast.get("target_metric"),
        "train_rmse": forecast.get("train_rmse"),
        "train_mae": forecast.get("train_mae"),
        "validation_rmse": forecast.get("validation_rmse"),
        "validation_mae": forecast.get("validation_mae"),
        "walk_forward_val_rmse": forecast.get("walk_forward_val_rmse"),
        "walk_forward_val_mae": forecast.get("walk_forward_val_mae"),
        "n_train": forecast.get("n_train"),
        "n_val": forecast.get("n_val"),
        "backtest": forecast.get("backtest"),
    }
    (artifacts_dir / "forecast_summary.json").write_text(json.dumps(artifact_payload, indent=2))
    LOGGER.info("Run complete. report=%s", report_path)
    return run_id, report_path


def run_backfill(thesis_name: str, iterations: int) -> list[str]:
    """
    Sequentially run the full pipeline N times. Historical date-aware backfill
    in connectors is not part of v1; this is used to accumulate paper-trade
    history, stress runs, and CI state growth.
    """
    run_ids: list[str] = []
    for _ in range(max(1, iterations)):
        run_id, _ = run_pipeline(thesis_name=thesis_name)
        run_ids.append(run_id)
    return run_ids
