import hashlib
import json
import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from src.core.config import get_settings
from src.core.logging import setup_logging
from src.core.schemas import PositionMark, RunManifest
from src.core.storage import Storage
from src.pipeline.paper_trading import apply_dedup, apply_exits, open_positions_by_family
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


def run_pipeline(thesis_name: str = "economic_indicators") -> tuple[str, Path | None]:
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

    # Phase 1: re-mark any existing open positions at the current mid before
    # creating new entries so unrealized PnL is always current.
    marks = [
        PositionMark(
            contract_id=snap.contract_id,
            venue=snap.venue,
            mark_price=snap.mid_price,
        )
        for snap in snapshots
    ]
    marked = storage.mark_open_positions(marks)
    LOGGER.info("Re-marked %d open position rows", marked)

    # Phase 2: evaluate exit rules against open positions before creating new entries.
    open_positions = storage.get_open_positions()
    closes = apply_exits(open_positions, signals, snapshots, settings)
    closed = storage.close_positions(closes)
    LOGGER.info("Closed %d positions this run", closed)

    # Sort by absolute edge descending so the highest-conviction signals are
    # processed first when the portfolio cap (paper_max_total_open) is active.
    signals_sorted = sorted(signals, key=lambda s: abs(s.edge_bps or 0), reverse=True)
    orders, candidate_positions = thesis.paper_trade(signals_sorted)

    # Phase 3: dedup — merge candidates into existing open positions when enabled.
    # Exclude positions just closed this run so they aren't treated as merge targets.
    closed_ids = {c.position_id for c in closes}
    live_positions = [p for p in open_positions if p.position_id not in closed_ids]

    existing_by_key = {
        (p.contract_id, p.venue, p.direction): p
        for p in live_positions
        if p.direction is not None
    }

    # Count ALL live open rows per (contract_id, venue, direction) key, including
    # legacy rows with direction=NULL (stored under the "" sentinel). This lets
    # apply_dedup() enforce paper_max_open_per_key even when old null-direction
    # positions are invisible to the key-lookup dict above.
    open_counts_by_key: dict[tuple[str, str, str], int] = {}
    for p in live_positions:
        k = (p.contract_id, p.venue, p.direction or "")
        open_counts_by_key[k] = open_counts_by_key.get(k, 0) + 1

    open_family_counts = open_positions_by_family(live_positions)
    new_positions, add_tos, acted_signal_ids = apply_dedup(
        candidate_positions, existing_by_key, settings, open_counts_by_key, open_family_counts
    )
    for add_to in add_tos:
        storage.add_to_position(add_to)
    LOGGER.info(
        "Dedup: %d new positions, %d merged into existing", len(new_positions), len(add_tos)
    )

    # Only record orders that resulted in an actual position open or VWAP merge.
    # Dropping dedup-blocked orders keeps paper_orders and paper_positions counts
    # consistent and prevents phantom fills from inflating the weekly digest totals.
    filled_orders = [o for o in orders if o.signal_id in acted_signal_ids]
    LOGGER.info(
        "Orders: %d generated, %d inserted (dedup dropped %d)",
        len(orders),
        len(filled_orders),
        len(orders) - len(filled_orders),
    )

    storage.insert_model_forecasts(forecast_records)
    storage.insert_signals(signals)
    storage.insert_snapshots(snapshots)
    storage.insert_orders(filled_orders)
    storage.insert_positions(new_positions)

    manifest.completed_at_utc = datetime.now(timezone.utc)
    storage.upsert_run_manifest(manifest)
    storage.close()

    if not settings.save_run_artifacts:
        LOGGER.info("Run complete. run_id=%s (artifact writing disabled)", run_id)
        return run_id, None

    reports_dir = settings.data_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    report_path = reports_dir / f"run_report_{run_id}.md"
    report_path.write_text(generate_run_report(str(settings.duckdb_path), run_id))
    report_html_path = reports_dir / f"run_report_{run_id}.html"
    report_html_path.write_text(generate_run_report_html(str(settings.duckdb_path), run_id))

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
