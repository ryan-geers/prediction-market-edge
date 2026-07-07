import hashlib
import json
import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from src.core.market_quotes import assess_yes_quote
from src.core.config import get_settings
from src.core.logging import setup_logging
from src.core.schemas import PaperPositionRecord, PositionClose, PositionMark, RunManifest
from src.core.storage import Storage
from src.pipeline.paper_trading import apply_dedup, apply_exits, open_positions_by_family
from src.pipeline.reporting import generate_run_report, generate_run_report_html
from src.theses.registry import build_registry

LOGGER = logging.getLogger(__name__)


def _is_kalshi_venue(venue: str | None) -> bool:
    return (venue or "").lower() == "kalshi"


def _settlement_exit_price(position: PaperPositionRecord, result: str) -> float:
    direction = position.direction or "yes"
    if result == "void":
        return position.avg_entry_price
    if direction == "yes":
        return 1.0 if result == "yes" else 0.0
    return 1.0 if result == "no" else 0.0


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

    # Phase 1: re-mark open positions using conservative bid/ask (not naive mid).
    marks = []
    for snap in snapshots:
        qa = assess_yes_quote(snap.best_bid, snap.best_ask, snap.last_trade, settings)
        marks.append(
            PositionMark(
                contract_id=snap.contract_id,
                venue=snap.venue,
                mark_price=qa.fair_yes_mid if qa.fair_yes_mid is not None else snap.mid_price,
                yes_bid=qa.best_bid,
                yes_ask=qa.best_ask,
                quote_reliable=qa.is_exit_quality or qa.fair_yes_mid is not None,
            )
        )
    marked = storage.mark_open_positions(marks)
    LOGGER.info("Re-marked %d open position rows", marked)

    # Phase 1b: resolve stale positions — contracts that haven't appeared in
    # any live snapshot for longer than paper_stale_position_close_hours have
    # almost certainly expired or been de-listed.  For Kalshi binary contracts
    # the outcome is definitive: YES pays $1 or $0 at settlement.  We check
    # the Kalshi API first so each position is closed at the true settlement
    # price rather than the last-known mark.  Kalshi rows with pending outcomes
    # stay open; only non-Kalshi stale rows fall back to stale_no_market.
    if settings.paper_stale_position_close_hours > 0:
        stale_positions = storage.get_stale_open_positions(settings.paper_stale_position_close_hours)
        if stale_positions:
            LOGGER.info(
                "Phase 1b: %d stale position(s) to resolve (last_mark_time > %.0fh ago)",
                len(stale_positions),
                settings.paper_stale_position_close_hours,
            )

            # Fetch settlement results from Kalshi for each unique contract.
            # Re-use the connector that was already instantiated for market data
            # by pulling it from the thesis registry when available.
            kalshi_connector = None
            try:
                kalshi_connector = registry[thesis_name].kalshi  # type: ignore[attr-defined]
            except AttributeError:
                pass

            settlement_closes: list[PositionClose] = []
            resolved_ids: set[str] = set()
            now_utc = datetime.now(timezone.utc)

            if kalshi_connector is not None:
                # Deduplicate: one API call per contract_id.
                unique_contracts = {
                    p.contract_id for p in stale_positions if _is_kalshi_venue(p.venue)
                }
                contract_results: dict[str, str] = {}
                for ticker in unique_contracts:
                    result = kalshi_connector.fetch_market_result(ticker)
                    if result is not None:
                        contract_results[ticker] = result
                        LOGGER.info("Kalshi settlement: %s → %s", ticker, result)
                    else:
                        LOGGER.debug("Kalshi settlement: %s not yet resolved", ticker)

                for pos in stale_positions:
                    result = contract_results.get(pos.contract_id)
                    if result is None:
                        continue

                    # Compute direction-aware exit price. Voided contracts refund
                    # the paper entry price so they do not create artificial PnL.
                    exit_price = _settlement_exit_price(pos, result)

                    realized = (exit_price - pos.avg_entry_price) * pos.net_qty
                    settlement_closes.append(
                        PositionClose(
                            position_id=pos.position_id,
                            avg_exit_price=exit_price,
                            realized_pnl=realized,
                            close_reason="contract_settled",
                            closed_at_utc=now_utc,
                        )
                    )
                    resolved_ids.add(pos.position_id)

            if settlement_closes:
                settled_count = storage.close_positions(settlement_closes)
                LOGGER.info(
                    "Settled %d position(s) at contract resolution price", settled_count
                )

            # Kalshi positions with pending/unknown outcomes must not be closed
            # at stale marks.  Only explicitly selected non-Kalshi stale rows
            # fall back to last-known mark closure.
            unresolved = [p for p in stale_positions if p.position_id not in resolved_ids]
            fallback_stale_ids = [
                p.position_id for p in unresolved if not _is_kalshi_venue(p.venue)
            ]
            if fallback_stale_ids:
                stale_closed = storage.close_stale_positions(
                    settings.paper_stale_position_close_hours,
                    position_ids=fallback_stale_ids,
                )
                if stale_closed:
                    LOGGER.info(
                        "Auto-closed %d non-Kalshi stale position(s) at last mark "
                        "(last_mark_time > %.0fh ago)",
                        stale_closed,
                        settings.paper_stale_position_close_hours,
                    )
            pending_kalshi = len(unresolved) - len(fallback_stale_ids)
            if pending_kalshi:
                LOGGER.info(
                    "Left %d stale Kalshi position(s) open pending finalized settlement",
                    pending_kalshi,
                )

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
