#!/usr/bin/env python3
"""
PME database audit script.

Connects to pme.duckdb, runs a battery of health checks, and writes a
structured Markdown report.  Designed to be called by the GitHub Actions
audit job (or locally) after each pipeline run.

Exit codes
----------
0  — all checks passed (or only info-level findings)
1  — at least one WARNING-level finding
2  — at least one CRITICAL-level finding (supersedes 1)

Usage
-----
    python scripts/audit_db.py --db data/pme.duckdb --out data/reports/audit_latest.md
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

try:
    import duckdb
except ImportError:
    print("duckdb not installed — run `pip install duckdb`", file=sys.stderr)
    sys.exit(2)


SEV = Literal["CRITICAL", "WARNING", "INFO"]
_SEV_ORDER = {"CRITICAL": 2, "WARNING": 1, "INFO": 0}


@dataclass
class Finding:
    severity: SEV
    check: str
    table: str
    message: str
    detail: str = ""
    recommendation: str = ""


@dataclass
class AuditResult:
    db_path: str
    run_at: str
    findings: list[Finding] = field(default_factory=list)

    def add(self, severity: SEV, check: str, table: str, message: str,
            detail: str = "", recommendation: str = "") -> None:
        self.findings.append(Finding(severity, check, table, message, detail, recommendation))

    @property
    def worst_severity(self) -> SEV | None:
        if not self.findings:
            return None
        return max((f.severity for f in self.findings), key=lambda s: _SEV_ORDER[s])

    def exit_code(self) -> int:
        ws = self.worst_severity
        if ws == "CRITICAL":
            return 2
        if ws == "WARNING":
            return 1
        return 0


def _scalar(con: "duckdb.DuckDBPyConnection", sql: str, params: list | None = None) -> object:
    row = con.execute(sql, params or []).fetchone()
    return row[0] if row else None


def run_audit(db_path: str) -> AuditResult:
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    result = AuditResult(db_path=db_path, run_at=now_utc)
    con = duckdb.connect(db_path, read_only=True)

    # ------------------------------------------------------------------
    # 1. Run manifest freshness
    # ------------------------------------------------------------------
    last_completed = _scalar(con, "SELECT MAX(completed_at_utc) FROM run_manifest")
    incomplete = _scalar(con, "SELECT COUNT(*) FROM run_manifest WHERE completed_at_utc IS NULL")

    if incomplete and int(incomplete) > 0:
        result.add(
            "CRITICAL", "run_completeness", "run_manifest",
            f"{incomplete} run(s) never completed (completed_at_utc IS NULL).",
            recommendation="Investigate pipeline failures; check GitHub Actions logs.",
        )
    else:
        result.add("INFO", "run_completeness", "run_manifest", "All runs completed successfully.")

    if last_completed is not None:
        hours_since = (datetime.now(timezone.utc) - last_completed.replace(tzinfo=timezone.utc)).total_seconds() / 3600
        if hours_since > 8:
            result.add(
                "WARNING", "run_freshness", "run_manifest",
                f"Last completed run was {hours_since:.1f}h ago (>{8}h threshold).",
                detail=f"last_completed={last_completed}",
                recommendation="Check cron schedule and GitHub Actions pipeline status.",
            )
        else:
            result.add("INFO", "run_freshness", "run_manifest",
                       f"Last run completed {hours_since:.1f}h ago.")

    # ------------------------------------------------------------------
    # 2. Model forecast health
    # ------------------------------------------------------------------
    model_rows = con.execute("""
        SELECT model_version, thesis_module,
               COUNT(*) AS cnt,
               STDDEV(model_probability) AS stddev_prob,
               AVG(model_probability) AS avg_prob,
               AVG(validation_rmse) AS avg_rmse
        FROM model_forecasts
        WHERE created_at_utc >= NOW() - INTERVAL '7 days'
        GROUP BY model_version, thesis_module
    """).fetchall()

    if not model_rows:
        result.add("WARNING", "model_forecasts_recent", "model_forecasts",
                   "No model_forecasts rows in the last 7 days.",
                   recommendation="Verify thesis.build_forecast_records() is persisting records.")
    else:
        for model_version, thesis, cnt, stddev_prob, avg_prob, avg_rmse in model_rows:
            stddev_prob = float(stddev_prob or 0)
            avg_prob = float(avg_prob or 0.5)
            avg_rmse = float(avg_rmse or 0)
            if stddev_prob < 0.01:
                result.add(
                    "CRITICAL", "model_degeneracy", "model_forecasts",
                    f"{model_version} ({thesis}): model_probability stddev={stddev_prob:.4f} — "
                    f"model is outputting a constant and has learned nothing.",
                    detail=f"avg_prob={avg_prob:.4f}, avg_rmse={avg_rmse:.4f}, last_7d_rows={cnt}",
                    recommendation=(
                        "Check model training pipeline; verify that training data is non-empty "
                        "and that the target variable has variance."
                    ),
                )
            elif avg_prob < 0.05 or avg_prob > 0.95:
                result.add(
                    "WARNING", "model_calibration", "model_forecasts",
                    f"{model_version} ({thesis}): avg model_probability={avg_prob:.3f} — "
                    f"model may be systematically miscalibrated.",
                    detail=f"stddev={stddev_prob:.4f}, avg_rmse={avg_rmse:.4f}, last_7d_rows={cnt}",
                    recommendation=(
                        "Investigate training data recency and feature scaling; compare "
                        "model predictions against recent actuals."
                    ),
                )
            else:
                result.add("INFO", "model_calibration", "model_forecasts",
                           f"{model_version}: avg_prob={avg_prob:.3f}, stddev={stddev_prob:.4f} (healthy).")

    # ------------------------------------------------------------------
    # 3. Position opening drought
    # ------------------------------------------------------------------
    last_opened = _scalar(con, "SELECT MAX(opened_at_utc) FROM paper_positions")
    enter_signals_recent = _scalar(con, """
        SELECT COUNT(*) FROM signals
        WHERE decision IN ('enter_long_yes', 'enter_long_no')
          AND event_time_utc >= NOW() - INTERVAL '24 hours'
    """)

    if last_opened is not None:
        hours_no_new = (datetime.now(timezone.utc) - last_opened.replace(tzinfo=timezone.utc)).total_seconds() / 3600
        if hours_no_new > 48 and int(enter_signals_recent or 0) > 0:
            result.add(
                "CRITICAL", "position_opening_drought", "paper_positions",
                f"No new positions opened for {hours_no_new:.0f}h despite "
                f"{enter_signals_recent} enter signals in the last 24h.",
                detail=f"last_opened={last_opened}",
                recommendation=(
                    "Check paper_max_total_open setting vs current open position count. "
                    "Run pme consolidate-positions if duplicate positions exist."
                ),
            )
        elif hours_no_new > 24:
            result.add(
                "WARNING", "position_opening_drought", "paper_positions",
                f"No new positions opened in {hours_no_new:.0f}h.",
                detail=f"last_opened={last_opened}",
            )
        else:
            result.add("INFO", "position_opening_drought", "paper_positions",
                       f"Last position opened {hours_no_new:.1f}h ago.")

    # ------------------------------------------------------------------
    # 4. Mark staleness
    # ------------------------------------------------------------------
    max_mark_age_h = _scalar(con, """
        SELECT MAX(EXTRACT(EPOCH FROM (NOW() - last_mark_time_utc)) / 3600)
        FROM paper_positions
        WHERE status = 'open' AND last_mark_time_utc IS NOT NULL
    """)
    open_null_mark = _scalar(con, """
        SELECT COUNT(*) FROM paper_positions
        WHERE status = 'open' AND last_mark_time_utc IS NULL
    """)

    if max_mark_age_h is not None and float(max_mark_age_h) > 48:
        result.add(
            "CRITICAL", "mark_staleness", "paper_positions",
            f"Oldest mark is {float(max_mark_age_h):.0f}h stale — unrealized PnL is unreliable.",
            recommendation="Verify mark_open_positions() runs every pipeline cycle.",
        )
    elif max_mark_age_h is not None and float(max_mark_age_h) > 24:
        result.add(
            "WARNING", "mark_staleness", "paper_positions",
            f"Oldest mark is {float(max_mark_age_h):.0f}h stale.",
        )
    else:
        result.add("INFO", "mark_staleness", "paper_positions",
                   f"Marks fresh (oldest {float(max_mark_age_h or 0):.1f}h).")

    if open_null_mark and int(open_null_mark) > 0:
        result.add(
            "WARNING", "mark_null", "paper_positions",
            f"{open_null_mark} open position(s) have never been marked.",
        )

    # ------------------------------------------------------------------
    # 5. Source latency instrumentation
    # ------------------------------------------------------------------
    total_snaps = _scalar(con, "SELECT COUNT(*) FROM market_snapshots")
    zero_latency = _scalar(con, "SELECT COUNT(*) FROM market_snapshots WHERE source_latency_ms = 0")

    if total_snaps and int(total_snaps) > 0:
        pct_zero = int(zero_latency or 0) / int(total_snaps) * 100
        if pct_zero > 90:
            result.add(
                "WARNING", "latency_instrumentation", "market_snapshots",
                f"{pct_zero:.0f}% of market snapshots have source_latency_ms=0 — "
                "API latency is not being measured.",
                recommendation="Verify connector records wall-clock time around HTTP calls.",
            )
        else:
            result.add("INFO", "latency_instrumentation", "market_snapshots",
                       f"{100-pct_zero:.0f}% of snapshots have non-zero latency.")

    # ------------------------------------------------------------------
    # 6. Illiquid signals
    # ------------------------------------------------------------------
    illiquid_enters = _scalar(con, """
        SELECT COUNT(*) FROM signals
        WHERE decision IN ('enter_long_yes', 'enter_long_no')
          AND spread_bps > 5000
          AND event_time_utc >= NOW() - INTERVAL '24 hours'
    """)
    if illiquid_enters and int(illiquid_enters) > 0:
        result.add(
            "WARNING", "illiquid_signals", "signals",
            f"{illiquid_enters} enter signal(s) in the last 24h on contracts with spread>5,000 bps.",
            recommendation="Raise market_max_spread_bps_hard or verify the hard-cap filter is active.",
        )
    else:
        result.add("INFO", "illiquid_signals", "signals",
                   "No illiquid enter signals (spread>5,000 bps) in the last 24h.")

    # ------------------------------------------------------------------
    # 7. Ghost positions (net_qty <= 0 and status = 'open')
    # ------------------------------------------------------------------
    ghost_open = _scalar(con, """
        SELECT COUNT(*) FROM paper_positions WHERE status = 'open' AND net_qty <= 0
    """)
    if ghost_open and int(ghost_open) > 0:
        result.add(
            "WARNING", "ghost_positions", "paper_positions",
            f"{ghost_open} open position(s) have net_qty<=0 — ledger artefacts.",
            recommendation="Run pme consolidate-positions to trigger the zero-qty cleanup sweep.",
        )
    else:
        result.add("INFO", "ghost_positions", "paper_positions", "No ghost open positions.")

    # ------------------------------------------------------------------
    # 8. Signal data quality
    # ------------------------------------------------------------------
    null_quality = _scalar(con, """
        SELECT COUNT(*) FROM signals
        WHERE event_time_utc >= NOW() - INTERVAL '7 days'
          AND (model_probability IS NULL OR market_implied_probability IS NULL OR edge_bps IS NULL)
    """)
    if null_quality and int(null_quality) > 0:
        result.add(
            "WARNING", "signal_null_fields", "signals",
            f"{null_quality} signal(s) in the last 7 days with NULL probability or edge fields.",
        )
    else:
        result.add("INFO", "signal_null_fields", "signals",
                   "No NULL probability/edge fields in last 7 days.")

    # ------------------------------------------------------------------
    # 9. Decision_reason format (should be valid JSON going forward)
    # ------------------------------------------------------------------
    recent_non_json = _scalar(con, """
        SELECT COUNT(*) FROM signals
        WHERE event_time_utc >= NOW() - INTERVAL '24 hours'
          AND decision_reason IS NOT NULL
          AND (NOT starts_with(trim(decision_reason), '{'))
    """)
    if recent_non_json and int(recent_non_json) > 0:
        result.add(
            "WARNING", "decision_reason_format", "signals",
            f"{recent_non_json} signal(s) in the last 24h still use the legacy semicolon "
            "decision_reason format instead of JSON.",
            recommendation="Verify the latest module.py changes are deployed.",
        )
    else:
        result.add("INFO", "decision_reason_format", "signals",
                   "All recent signals use JSON decision_reason format.")

    # ------------------------------------------------------------------
    # 10. Portfolio cap utilisation
    # ------------------------------------------------------------------
    total_open = _scalar(con, "SELECT COUNT(*) FROM paper_positions WHERE status = 'open'")
    if total_open:
        result.add("INFO", "portfolio_size", "paper_positions",
                   f"{total_open} open position(s) in the book.")

    con.close()
    return result


def render_markdown(result: AuditResult) -> str:
    lines = []
    lines.append(f"# PME Database Audit — {result.run_at}")
    lines.append(f"\n**Source:** `{result.db_path}`\n")

    worst = result.worst_severity
    badge = {"CRITICAL": "🔴 CRITICAL", "WARNING": "🟡 WARNING", "INFO": "🟢 PASS"}.get(worst or "INFO", "🟢 PASS")
    lines.append(f"**Overall status:** {badge}\n")

    by_sev: dict[str, list[Finding]] = {"CRITICAL": [], "WARNING": [], "INFO": []}
    for f in result.findings:
        by_sev[f.severity].append(f)

    for sev in ("CRITICAL", "WARNING", "INFO"):
        items = by_sev[sev]
        if not items:
            continue
        lines.append(f"\n## {sev} ({len(items)})\n")
        for f in items:
            lines.append(f"### `{f.check}` — {f.table}")
            lines.append(f"\n{f.message}")
            if f.detail:
                lines.append(f"\n> {f.detail}")
            if f.recommendation:
                lines.append(f"\n**Fix:** {f.recommendation}")
            lines.append("")

    lines.append("\n---")
    lines.append(f"*Generated by `scripts/audit_db.py` at {result.run_at}*")
    return "\n".join(lines)


def render_github_summary(result: AuditResult) -> str:
    """Compact single-table summary for $GITHUB_STEP_SUMMARY."""
    lines = [
        f"## PME Audit — {result.run_at}",
        "",
        "| Severity | Check | Table | Message |",
        "| -------- | ----- | ----- | ------- |",
    ]
    for f in sorted(result.findings, key=lambda x: (-_SEV_ORDER[x.severity], x.check)):
        icon = {"CRITICAL": "🔴", "WARNING": "🟡", "INFO": "🟢"}[f.severity]
        msg = f.message.replace("|", "\\|").replace("\n", " ")[:120]
        lines.append(f"| {icon} {f.severity} | `{f.check}` | `{f.table}` | {msg} |")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit the PME DuckDB file.")
    parser.add_argument("--db", default="data/pme.duckdb", help="Path to pme.duckdb")
    parser.add_argument("--out", default=None, help="Write Markdown report to this path")
    parser.add_argument("--github-summary", action="store_true",
                        help="Also write a compact summary to $GITHUB_STEP_SUMMARY")
    parser.add_argument("--json", dest="json_out", default=None,
                        help="Write findings as JSON to this path")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"ERROR: database not found at {db_path}", file=sys.stderr)
        sys.exit(2)

    result = run_audit(str(db_path))
    md = render_markdown(result)

    print(md)

    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(md, encoding="utf-8")
        print(f"\nReport written to {args.out}", file=sys.stderr)

    if args.github_summary:
        import os
        summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
        if summary_path:
            with open(summary_path, "a", encoding="utf-8") as fh:
                fh.write("\n" + render_github_summary(result) + "\n")

    if args.json_out:
        findings_data = [
            {
                "severity": f.severity,
                "check": f.check,
                "table": f.table,
                "message": f.message,
                "detail": f.detail,
                "recommendation": f.recommendation,
            }
            for f in result.findings
        ]
        Path(args.json_out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.json_out).write_text(
            json.dumps({"run_at": result.run_at, "db_path": result.db_path,
                        "worst_severity": result.worst_severity, "findings": findings_data},
                       indent=2),
            encoding="utf-8",
        )

    sys.exit(result.exit_code())


if __name__ == "__main__":
    main()
