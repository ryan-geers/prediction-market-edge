from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import duckdb

from src.core.schemas import (
    AddToPosition,
    MarketSnapshotRecord,
    ModelForecastRecord,
    PaperOrderRecord,
    PaperPositionRecord,
    PositionClose,
    PositionMark,
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
    # Phase 2: direction is required for exit-flip logic; Phase 3 uses it for dedup.
    "ALTER TABLE paper_positions ADD COLUMN IF NOT EXISTS direction VARCHAR",
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
                  last_mark_time_utc, status, close_reason, direction
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    p["direction"],
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

    def get_open_positions(self) -> list[PaperPositionRecord]:
        """Return all currently-open paper positions."""
        rows = self.con.execute(
            """
            SELECT position_id, run_id, signal_id, venue, contract_id,
                   opened_at_utc, closed_at_utc, net_qty, avg_entry_price,
                   avg_exit_price, realized_pnl, unrealized_pnl, mark_price,
                   last_mark_time_utc, status, close_reason, direction
            FROM paper_positions
            WHERE status = 'open'
            """
        ).fetchall()
        return [
            PaperPositionRecord(
                position_id=r[0],
                run_id=r[1] or "",
                signal_id=r[2] or "",
                venue=r[3],
                contract_id=r[4],
                opened_at_utc=r[5],
                closed_at_utc=r[6],
                net_qty=r[7],
                avg_entry_price=r[8],
                avg_exit_price=r[9],
                realized_pnl=r[10] or 0.0,
                unrealized_pnl=r[11] or 0.0,
                mark_price=r[12],
                last_mark_time_utc=r[13],
                status=r[14],
                close_reason=r[15],
                direction=r[16],
            )
            for r in rows
        ]

    def close_positions(self, closes: Iterable[PositionClose]) -> int:
        """
        Mark positions as closed and record exit details.
        Returns the number of rows updated.
        """
        updated = 0
        for close in closes:
            rows = self.con.execute(
                """
                UPDATE paper_positions
                SET
                  status          = 'closed',
                  closed_at_utc   = ?,
                  avg_exit_price  = ?,
                  realized_pnl    = ?,
                  unrealized_pnl  = 0.0,
                  close_reason    = ?
                WHERE position_id = ?
                RETURNING position_id
                """,
                [
                    close.closed_at_utc,
                    close.avg_exit_price,
                    close.realized_pnl,
                    close.close_reason,
                    close.position_id,
                ],
            ).fetchall()
            updated += len(rows)
        return updated

    def get_open_position(
        self, contract_id: str, venue: str, direction: str
    ) -> PaperPositionRecord | None:
        """Return the single open position for (contract_id, venue, direction), or None."""
        row = self.con.execute(
            """
            SELECT position_id, run_id, signal_id, venue, contract_id,
                   opened_at_utc, closed_at_utc, net_qty, avg_entry_price,
                   avg_exit_price, realized_pnl, unrealized_pnl, mark_price,
                   last_mark_time_utc, status, close_reason, direction
            FROM paper_positions
            WHERE status       = 'open'
              AND contract_id  = ?
              AND venue        = ?
              AND direction    = ?
            LIMIT 1
            """,
            [contract_id, venue, direction],
        ).fetchone()
        if row is None:
            return None
        return PaperPositionRecord(
            position_id=row[0],
            run_id=row[1] or "",
            signal_id=row[2] or "",
            venue=row[3],
            contract_id=row[4],
            opened_at_utc=row[5],
            closed_at_utc=row[6],
            net_qty=row[7],
            avg_entry_price=row[8],
            avg_exit_price=row[9],
            realized_pnl=row[10] or 0.0,
            unrealized_pnl=row[11] or 0.0,
            mark_price=row[12],
            last_mark_time_utc=row[13],
            status=row[14],
            close_reason=row[15],
            direction=row[16],
        )

    def add_to_position(self, op: AddToPosition) -> bool:
        """
        VWAP-merge a new fill into an existing open position.
        Updates net_qty, avg_entry_price, and recomputes unrealized_pnl from the
        current mark_price (which Phase 1 already refreshed this run).
        Returns True if a row was updated.
        """
        rows = self.con.execute(
            """
            UPDATE paper_positions
            SET
              net_qty         = ?,
              avg_entry_price = ?,
              unrealized_pnl  = (mark_price - ?) * ?
            WHERE position_id = ?
            RETURNING position_id
            """,
            [
                op.new_net_qty,
                op.new_avg_entry_price,
                op.new_avg_entry_price,
                op.new_net_qty,
                op.position_id,
            ],
        ).fetchall()
        return len(rows) > 0

    def mark_open_positions(self, marks: Iterable[PositionMark]) -> int:
        """
        Re-price all open positions for the given contracts at the current YES mid.
        NO positions are marked in NO-price space (1 - YES mid), matching their
        entry price basis.
        Returns the total number of rows updated.
        Uses RETURNING to get an accurate count because DuckDB always reports
        rowcount=-1 for UPDATE statements.
        """
        updated = 0
        for mark in marks:
            rows = self.con.execute(
                """
                UPDATE paper_positions
                SET
                  mark_price           = CASE
                    WHEN direction = 'no' THEN 1.0 - ?
                    ELSE ?
                  END,
                  unrealized_pnl       = (
                    CASE
                      WHEN direction = 'no' THEN 1.0 - ?
                      ELSE ?
                    END - avg_entry_price
                  ) * net_qty,
                  last_mark_time_utc   = ?
                WHERE status       = 'open'
                  AND contract_id  = ?
                  AND venue        = ?
                RETURNING position_id
                """,
                [
                    mark.mark_price,
                    mark.mark_price,
                    mark.mark_price,
                    mark.mark_price,
                    mark.last_mark_time_utc,
                    mark.contract_id,
                    mark.venue,
                ],
            ).fetchall()
            updated += len(rows)
        return updated

    def consolidate_duplicate_positions(self) -> list[dict]:
        """
        Merge duplicate open positions that share (contract_id, venue, direction).

        For each such group the oldest position (by opened_at_utc) is kept as the
        survivor. All duplicates are VWAP-merged into it (qty summed, avg entry
        price VWAP-weighted) and then marked closed with
        close_reason='dedup_consolidated' and realized_pnl=0 so they no longer
        appear in the open book.

        Returns a list of summary dicts, one per consolidated group.
        """
        groups = self.con.execute(
            """
            SELECT contract_id, venue, direction, COUNT(*) AS cnt
            FROM paper_positions
            WHERE status = 'open' AND direction IS NOT NULL
            GROUP BY contract_id, venue, direction
            HAVING COUNT(*) > 1
            """
        ).fetchall()

        summaries: list[dict] = []
        now = datetime.now(timezone.utc)

        for contract_id, venue, direction, cnt in groups:
            rows = self.con.execute(
                """
                SELECT position_id, net_qty, avg_entry_price, mark_price
                FROM paper_positions
                WHERE status = 'open'
                  AND contract_id = ?
                  AND venue       = ?
                  AND direction   = ?
                ORDER BY opened_at_utc ASC NULLS LAST
                """,
                [contract_id, venue, direction],
            ).fetchall()

            if len(rows) <= 1:
                continue

            survivor_id = rows[0][0]

            total_qty = sum(r[1] for r in rows)
            if total_qty > 0:
                vwap_price = sum(r[1] * r[2] for r in rows) / total_qty
            else:
                vwap_price = rows[0][2]

            mark = rows[0][3]
            new_unrealized = (mark - vwap_price) * total_qty if mark is not None else 0.0

            self.con.execute(
                """
                UPDATE paper_positions
                SET net_qty        = ?,
                    avg_entry_price = ?,
                    unrealized_pnl  = ?
                WHERE position_id = ?
                """,
                [total_qty, vwap_price, new_unrealized, survivor_id],
            )

            for dup_id, _, dup_entry, _ in rows[1:]:
                self.con.execute(
                    """
                    UPDATE paper_positions
                    SET status         = 'closed',
                        closed_at_utc  = ?,
                        avg_exit_price = avg_entry_price,
                        realized_pnl   = 0.0,
                        unrealized_pnl = 0.0,
                        net_qty        = 0.0,
                        close_reason   = 'dedup_consolidated'
                    WHERE position_id = ?
                    """,
                    [now, dup_id],
                )

            summaries.append(
                {
                    "contract_id": contract_id,
                    "venue": venue,
                    "direction": direction,
                    "positions_merged": cnt,
                    "survivor_position_id": survivor_id,
                    "consolidated_qty": total_qty,
                    "vwap_avg_entry": vwap_price,
                }
            )

        return summaries

    def close(self) -> None:
        self.con.close()
