from pathlib import Path
from typing import Iterable

import duckdb

from src.core.schemas import (
    MarketSnapshotRecord,
    ModelForecastRecord,
    PaperOrderRecord,
    PaperPositionRecord,
    RunManifest,
    SignalRecord,
)


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS signals (
  signal_id VARCHAR PRIMARY KEY,
  run_id VARCHAR,
  thesis_module VARCHAR,
  venue VARCHAR,
  contract_id VARCHAR,
  contract_label VARCHAR,
  event_time_utc TIMESTAMP,
  ingested_at_utc TIMESTAMP,
  model_probability DOUBLE,
  market_implied_probability DOUBLE,
  edge_bps DOUBLE,
  bid_price DOUBLE,
  ask_price DOUBLE,
  spread_bps DOUBLE,
  vig_adjusted_threshold_bps DOUBLE,
  decision VARCHAR,
  decision_reason VARCHAR,
  model_version VARCHAR,
  feature_set_version VARCHAR,
  assumption_version VARCHAR
);

CREATE TABLE IF NOT EXISTS paper_orders (
  paper_order_id VARCHAR PRIMARY KEY,
  signal_id VARCHAR,
  run_id VARCHAR,
  venue VARCHAR,
  contract_id VARCHAR,
  side VARCHAR,
  order_type VARCHAR,
  qty DOUBLE,
  limit_price DOUBLE,
  fill_price DOUBLE,
  fill_qty DOUBLE,
  status VARCHAR,
  submitted_at_utc TIMESTAMP,
  filled_at_utc TIMESTAMP,
  cancelled_at_utc TIMESTAMP,
  fill_rule VARCHAR,
  slippage_assumption_bps DOUBLE,
  fees_assumption_bps DOUBLE,
  assumption_version VARCHAR,
  slippage_model_name VARCHAR
);

CREATE TABLE IF NOT EXISTS paper_positions (
  position_id VARCHAR PRIMARY KEY,
  run_id VARCHAR,
  signal_id VARCHAR,
  venue VARCHAR,
  contract_id VARCHAR,
  opened_at_utc TIMESTAMP,
  closed_at_utc TIMESTAMP,
  net_qty DOUBLE,
  avg_entry_price DOUBLE,
  avg_exit_price DOUBLE,
  realized_pnl DOUBLE,
  unrealized_pnl DOUBLE,
  mark_price DOUBLE,
  last_mark_time_utc TIMESTAMP,
  status VARCHAR,
  close_reason VARCHAR
);

CREATE TABLE IF NOT EXISTS market_snapshots (
  snapshot_id VARCHAR PRIMARY KEY,
  venue VARCHAR,
  contract_id VARCHAR,
  snapshot_time_utc TIMESTAMP,
  best_bid DOUBLE,
  best_ask DOUBLE,
  last_trade DOUBLE,
  mid_price DOUBLE,
  spread_bps DOUBLE,
  orderbook_depth_json VARCHAR,
  source_latency_ms BIGINT
);

CREATE TABLE IF NOT EXISTS run_manifest (
  run_id VARCHAR PRIMARY KEY,
  code_commit_sha VARCHAR,
  config_hash VARCHAR,
  active_thesis VARCHAR,
  data_sources VARCHAR,
  started_at_utc TIMESTAMP,
  completed_at_utc TIMESTAMP
);

CREATE TABLE IF NOT EXISTS model_forecasts (
  forecast_id VARCHAR PRIMARY KEY,
  run_id VARCHAR,
  thesis_module VARCHAR,
  release_date_utc TIMESTAMP,
  model_probability DOUBLE,
  target_metric VARCHAR,
  model_version VARCHAR,
  feature_set_version VARCHAR,
  training_start_utc TIMESTAMP,
  training_end_utc TIMESTAMP,
  validation_rmse DOUBLE,
  validation_mae DOUBLE,
  created_at_utc TIMESTAMP
);
"""

SCHEMA_MIGRATIONS = [
    "ALTER TABLE paper_orders ADD COLUMN IF NOT EXISTS assumption_version VARCHAR",
    "ALTER TABLE paper_orders ADD COLUMN IF NOT EXISTS slippage_model_name VARCHAR",
    "ALTER TABLE paper_positions ADD COLUMN IF NOT EXISTS run_id VARCHAR",
    "ALTER TABLE paper_positions ADD COLUMN IF NOT EXISTS signal_id VARCHAR",
]


class Storage:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.con = duckdb.connect(str(db_path))
        self.con.execute(SCHEMA_SQL)
        for stmt in SCHEMA_MIGRATIONS:
            self.con.execute(stmt)

    def upsert_run_manifest(self, manifest: RunManifest) -> None:
        self.con.execute(
            """
            INSERT OR REPLACE INTO run_manifest VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            list(manifest.model_dump().values()),
        )

    def insert_signals(self, signals: Iterable[SignalRecord]) -> None:
        for signal in signals:
            self.con.execute(
                "INSERT INTO signals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                list(signal.model_dump().values()),
            )

    def insert_orders(self, orders: Iterable[PaperOrderRecord]) -> None:
        for order in orders:
            o = order.model_dump()
            self.con.execute(
                """
                INSERT INTO paper_orders (
                  paper_order_id, signal_id, run_id, venue, contract_id, side, order_type, qty,
                  limit_price, fill_price, fill_qty, status, submitted_at_utc, filled_at_utc, cancelled_at_utc,
                  fill_rule, slippage_assumption_bps, fees_assumption_bps, assumption_version, slippage_model_name
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    o["paper_order_id"],
                    o["signal_id"],
                    o["run_id"],
                    o["venue"],
                    o["contract_id"],
                    o["side"],
                    o["order_type"],
                    o["qty"],
                    o["limit_price"],
                    o["fill_price"],
                    o["fill_qty"],
                    o["status"],
                    o["submitted_at_utc"],
                    o["filled_at_utc"],
                    o["cancelled_at_utc"],
                    o["fill_rule"],
                    o["slippage_assumption_bps"],
                    o["fees_assumption_bps"],
                    o["assumption_version"],
                    o["slippage_model_name"],
                ],
            )

    def insert_positions(self, positions: Iterable[PaperPositionRecord]) -> None:
        for position in positions:
            p = position.model_dump()
            self.con.execute(
                """
                INSERT INTO paper_positions (
                  position_id, run_id, signal_id, venue, contract_id, opened_at_utc, closed_at_utc,
                  net_qty, avg_entry_price, avg_exit_price, realized_pnl, unrealized_pnl, mark_price,
                  last_mark_time_utc, status, close_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    p["position_id"],
                    p["run_id"] or None,
                    p["signal_id"] or None,
                    p["venue"],
                    p["contract_id"],
                    p["opened_at_utc"],
                    p["closed_at_utc"],
                    p["net_qty"],
                    p["avg_entry_price"],
                    p["avg_exit_price"],
                    p["realized_pnl"],
                    p["unrealized_pnl"],
                    p["mark_price"],
                    p["last_mark_time_utc"],
                    p["status"],
                    p["close_reason"],
                ],
            )

    def insert_snapshots(self, snapshots: Iterable[MarketSnapshotRecord]) -> None:
        for snapshot in snapshots:
            self.con.execute(
                "INSERT INTO market_snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                list(snapshot.model_dump().values()),
            )

    def insert_model_forecasts(self, forecasts: Iterable[ModelForecastRecord]) -> None:
        for forecast in forecasts:
            self.con.execute(
                "INSERT INTO model_forecasts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                list(forecast.model_dump().values()),
            )

    def close(self) -> None:
        self.con.close()
