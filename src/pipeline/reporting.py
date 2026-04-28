from __future__ import annotations

import html as html_lib
from datetime import datetime, timedelta, timezone
from typing import Any

import duckdb


def _escape(s: Any) -> str:
    if s is None:
        return ""
    return html_lib.escape(str(s), quote=True)


def _load_run_report_data(con: duckdb.DuckDBPyConnection, run_id: str) -> dict[str, Any]:
    counts = con.execute(
        """
        SELECT decision, COUNT(*) AS n
        FROM signals
        WHERE run_id = ?
        GROUP BY 1
        ORDER BY 2 DESC
        """,
        [run_id],
    ).fetchall()
    pnl = con.execute(
        """
        SELECT COALESCE(SUM(COALESCE(unrealized_pnl, 0) + COALESCE(realized_pnl, 0)), 0)
        FROM paper_positions
        WHERE run_id = ?
        """,
        [run_id],
    ).fetchone()[0]
    order_n = con.execute(
        "SELECT COUNT(*) FROM paper_orders WHERE run_id = ?",
        [run_id],
    ).fetchone()[0]
    fc = con.execute(
        "SELECT model_probability, validation_rmse, target_metric FROM model_forecasts WHERE run_id = ? LIMIT 1",
        [run_id],
    ).fetchone()
    return {"counts": counts, "pnl": float(pnl), "order_n": order_n, "fc": fc}


def generate_run_report(db_path: str, run_id: str) -> str:
    con = duckdb.connect(db_path)
    try:
        d = _load_run_report_data(con, run_id)
    finally:
        con.close()

    lines = ["# Run report", "", f"- **run_id:** `{run_id}`", "", "## Signal decisions"]
    for decision, n in d["counts"]:
        lines.append(f"- {decision}: {n}")
    lines.extend(
        [
            "",
            "## Paper trading",
            f"- Paper orders this run: {d['order_n']}",
            f"- Run-scoped PnL (unrealized + realized): {d['pnl']:.6f}",
        ]
    )
    fc = d["fc"]
    if fc:
        mp, rmse, tgt = fc[0], fc[1], fc[2]
        lines.extend(["", "## Model snapshot", f"- Model P(YES): {mp}", f"- Validation RMSE: {rmse}", f"- Target: {tgt}"])
    return "\n".join(lines)


def generate_run_report_html(db_path: str, run_id: str) -> str:
    """HTML run summary for archives and attachments."""
    con = duckdb.connect(db_path)
    try:
        d = _load_run_report_data(con, run_id)
    finally:
        con.close()
    rows = "".join(f"<tr><td>{_escape(dec)}</td><td>{n}</td></tr>" for dec, n in d["counts"])
    fc_block = ""
    if d["fc"]:
        mp, rmse, tgt = d["fc"][0], d["fc"][1], d["fc"][2]
        fc_block = f"<p>Model P(YES): {_escape(mp)} · Val RMSE: {_escape(rmse)} · Target: {_escape(tgt)}</p>"
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"/><title>Run report</title>
<style>body{{font-family:system-ui,sans-serif;max-width:720px;margin:1rem auto;}}
table{{border-collapse:collapse;width:100%}}td,th{{border:1px solid #ccc;padding:0.35rem 0.5rem;text-align:left}}th{{background:#f4f4f4}}</style>
</head><body>
<h1>Run report</h1>
<p><code>{_escape(run_id)}</code></p>
<h2>Signal decisions</h2>
<table><thead><tr><th>Decision</th><th>Count</th></tr></thead><tbody>{rows or '<tr><td colspan="2">(none)</td></tr>'}</tbody></table>
<h2>Paper trading</h2>
<p>Orders: {d['order_n']} · PnL (unreal + real): {_escape(_fmtf(d['pnl']))}</p>
{fc_block}
</body></html>
"""


def _weekly_payload(con: duckdb.DuckDBPyConnection, since: datetime) -> dict[str, Any]:
    signal_by_thesis = con.execute(
        """
        SELECT thesis_module, COUNT(*) AS n
        FROM signals WHERE event_time_utc >= ?
        GROUP BY 1 ORDER BY 2 DESC
        """,
        [since],
    ).fetchall()
    decisions = con.execute(
        """
        SELECT decision, COUNT(*) AS n FROM signals WHERE event_time_utc >= ?
        GROUP BY 1 ORDER BY 2 DESC
        """,
        [since],
    ).fetchall()
    contracts_n = con.execute(
        "SELECT COUNT(DISTINCT contract_id) FROM signals WHERE event_time_utc >= ?", [since]
    ).fetchone()[0]
    signals_n = con.execute("SELECT COUNT(*) FROM signals WHERE event_time_utc >= ?", [since]).fetchone()[0]
    edge = con.execute(
        """
        SELECT AVG(edge_bps), STDDEV(edge_bps), MIN(edge_bps), MAX(edge_bps)
        FROM signals WHERE event_time_utc >= ?
        """,
        [since],
    ).fetchone()
    order_count = con.execute(
        "SELECT COUNT(*) FROM paper_orders WHERE submitted_at_utc >= ?", [since]
    ).fetchone()[0]
    open_positions = con.execute(
        "SELECT COUNT(*) FROM paper_positions WHERE status = 'open'"
    ).fetchone()[0]
    pnl_window = con.execute(
        """
        SELECT
          COALESCE(SUM(COALESCE(realized_pnl,0) + COALESCE(unrealized_pnl,0)), 0),
          COALESCE(SUM(COALESCE(realized_pnl,0)), 0),
          COALESCE(SUM(COALESCE(unrealized_pnl,0)), 0)
        FROM paper_positions
        WHERE opened_at_utc >= ?
        """,
        [since],
    ).fetchone()
    closed = con.execute(
        """
        SELECT realized_pnl FROM paper_positions
        WHERE status = 'closed' AND COALESCE(closed_at_utc, opened_at_utc) >= ?
        """,
        [since],
    ).fetchall()
    hit_rate = None
    if closed:
        wins = sum(1 for (r,) in closed if r is not None and float(r) > 0)
        hit_rate = 100.0 * wins / len(closed)
    winners = con.execute(
        """
        SELECT contract_id, realized_pnl, run_id FROM paper_positions
        WHERE status = 'closed' AND realized_pnl IS NOT NULL
          AND COALESCE(closed_at_utc, opened_at_utc) >= ?
        ORDER BY realized_pnl DESC NULLS LAST LIMIT 5
        """,
        [since],
    ).fetchall()
    losers = con.execute(
        """
        SELECT contract_id, realized_pnl, run_id FROM paper_positions
        WHERE status = 'closed' AND realized_pnl IS NOT NULL
          AND COALESCE(closed_at_utc, opened_at_utc) >= ?
        ORDER BY realized_pnl ASC NULLS LAST LIMIT 5
        """,
        [since],
    ).fetchall()
    last_sig = con.execute("SELECT MAX(event_time_utc) FROM signals").fetchone()[0]
    runs_n = con.execute(
        "SELECT COUNT(*) FROM run_manifest WHERE COALESCE(completed_at_utc, started_at_utc) >= ?", [since]
    ).fetchone()[0]
    last_sources = con.execute(
        """
        SELECT data_sources FROM run_manifest
        ORDER BY COALESCE(completed_at_utc, started_at_utc) DESC NULLS LAST LIMIT 1
        """
    ).fetchone()
    return {
        "since": since,
        "signal_by_thesis": signal_by_thesis,
        "decisions": decisions,
        "contracts_n": contracts_n,
        "signals_n": signals_n,
        "edge_avg": edge[0] if edge else None,
        "edge_std": edge[1] if edge else None,
        "edge_min": edge[2] if edge else None,
        "edge_max": edge[3] if edge else None,
        "order_count": order_count,
        "open_positions": open_positions,
        "pnl_total": pnl_window[0] if pnl_window else 0,
        "pnl_realized": pnl_window[1] if pnl_window else 0,
        "pnl_unrealized": pnl_window[2] if pnl_window else 0,
        "hit_rate": hit_rate,
        "closed_n": len(closed),
        "winners": winners,
        "losers": losers,
        "last_sig": last_sig,
        "runs_n": runs_n,
        "last_sources": last_sources[0] if last_sources else None,
    }


def _fmtf(x: Any) -> str:
    if x is None:
        return "—"
    try:
        return f"{float(x):.6f}"
    except (TypeError, ValueError):
        return str(x)


def _format_weekly_md(w: dict[str, Any]) -> str:
    lines = [
        "# Weekly Prediction Market Edge digest",
        "",
        f"- **Window (UTC):** since `{w['since'].isoformat()}`",
        "",
        "## Signals",
        f"- Total signals: {w['signals_n']}",
        f"- Distinct contracts evaluated: {w['contracts_n']}",
        "",
        "### By thesis",
    ]
    for thesis, n in w["signal_by_thesis"]:
        lines.append(f"- {thesis}: {n}")
    lines.extend(["", "### Decisions"])
    for d, n in w["decisions"]:
        lines.append(f"- {d}: {n}")
    lines.extend(
        [
            "",
            "### Edge (bps) over the window",
            f"- Mean: {_fmtf(w['edge_avg'])} · Std: {_fmtf(w['edge_std'])} · Min: {_fmtf(w['edge_min'])} · Max: {_fmtf(w['edge_max'])}",
            "",
            "## Paper trading",
            f"- Orders submitted in window: {w['order_count']}",
            f"- Open positions (all time): {w['open_positions']}",
            f"- PnL from positions opened in window (unreal + real, sum): {_fmtf(w['pnl_total'])}",
            f"- Realized / unrealized components (same filter): {_fmtf(w['pnl_realized'])} / {_fmtf(w['pnl_unrealized'])}",
        ]
    )
    if w["hit_rate"] is not None:
        lines.append(f"- Closed positions in window: {w['closed_n']}; hit rate (realized > 0): {w['hit_rate']:.1f}%")
    lines.extend(["", "### Largest realized winners (window, top 5)"])
    if w["winners"]:
        for cid, rp, rid in w["winners"]:
            lines.append(f"- {cid} (run {rid}): {_fmtf(rp)}")
    else:
        lines.append("- (none)")
    lines.extend(["", "### Largest realized losers (window, top 5)"])
    if w["losers"]:
        for cid, rp, rid in w["losers"]:
            lines.append(f"- {cid} (run {rid}): {_fmtf(rp)}")
    else:
        lines.append("- (none)")
    lines.extend(
        [
            "",
            "## Data freshness",
            f"- Latest stored signal `event_time_utc`: {w['last_sig']}",
            f"- Pipeline runs with activity in window (manifest): {w['runs_n']}",
            f"- Latest run `data_sources`: {w['last_sources'] or '—'}",
            "",
            "## Connectors",
            "- Per-connector success/failure counts are not persisted in DuckDB v1; inspect CI logs.",
            "",
            "## Audit",
            "- Full machine-readable history: restore **`pme-state`** / `pme.duckdb` from workflow artifacts when available.",
        ]
    )
    return "\n".join(lines)


def _format_weekly_html(w: dict[str, Any]) -> str:
    rows_sig = "".join(f"<tr><td>{_escape(a)}</td><td>{n}</td></tr>" for a, n in w["signal_by_thesis"])
    rows_dec = "".join(f"<tr><td>{_escape(a)}</td><td>{n}</td></tr>" for a, n in w["decisions"])
    win_rows = "".join(
        f"<tr><td>{_escape(c)}</td><td>{_escape(r)}</td><td>{_escape(rp)}</td></tr>" for c, rp, r in w["winners"]
    )
    lose_rows = "".join(
        f"<tr><td>{_escape(c)}</td><td>{_escape(r)}</td><td>{_escape(rp)}</td></tr>" for c, rp, r in w["losers"]
    )
    hit = f"{w['hit_rate']:.1f}%" if w["hit_rate"] is not None else "n/a"
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"/><title>Weekly digest</title>
<style>body{{font-family:system-ui,sans-serif;max-width:900px;margin:1rem auto;}}
table{{border-collapse:collapse;width:100%;margin:0.5rem 0}}th,td{{border:1px solid #ccc;padding:0.35rem 0.5rem;text-align:left}}th{{background:#f4f4f4}}</style>
</head><body>
<h1>Weekly Prediction Market Edge digest</h1>
<p>Window since <code>{_escape(w['since'].isoformat())}</code></p>
<h2>Signals</h2>
<p>Total: {w['signals_n']} · Distinct contracts: {w['contracts_n']}</p>
<p>Edge bps — mean {_escape(_fmtf(w['edge_avg']))}, std {_escape(_fmtf(w['edge_std']))}, min {_escape(_fmtf(w['edge_min']))}, max {_escape(_fmtf(w['edge_max']))}</p>
<h3>By thesis</h3>
<table><thead><tr><th>Thesis</th><th>Signals</th></tr></thead><tbody>{rows_sig or '<tr><td colspan="2">(none)</td></tr>'}</tbody></table>
<h3>Decisions</h3>
<table><thead><tr><th>Decision</th><th>Count</th></tr></thead><tbody>{rows_dec or '<tr><td colspan="2">(none)</td></tr>'}</tbody></table>
<h2>Paper trading</h2>
<p>Orders in window: {w['order_count']} · Open positions (all time): {w['open_positions']}</p>
<p>PnL (positions opened in window): total {_escape(_fmtf(w['pnl_total']))} — realized {_escape(_fmtf(w['pnl_realized']))}, unrealized {_escape(_fmtf(w['pnl_unrealized']))}</p>
<p>Closed in window: {w['closed_n']} · Hit rate (realized &gt; 0): {hit}</p>
<h3>Top winners</h3>
<table><thead><tr><th>Contract</th><th>Run</th><th>Realized PnL</th></tr></thead><tbody>{win_rows or '<tr><td colspan="3">(none)</td></tr>'}</tbody></table>
<h3>Top losers</h3>
<table><thead><tr><th>Contract</th><th>Run</th><th>Realized PnL</th></tr></thead><tbody>{lose_rows or '<tr><td colspan="3">(none)</td></tr>'}</tbody></table>
<h2>Data freshness</h2>
<p>Latest signal time: {_escape(w['last_sig'])} · Runs in window: {w['runs_n']}</p>
<p>Latest sources: {_escape(w['last_sources'])}</p>
<p><em>Pull workflow artifacts for full DuckDB history.</em></p>
</body></html>
"""


def generate_weekly_digest(db_path: str) -> str:
    since = datetime.now(timezone.utc) - timedelta(days=7)
    con = duckdb.connect(db_path)
    try:
        w = _weekly_payload(con, since)
    finally:
        con.close()
    return _format_weekly_md(w)


def generate_weekly_digest_html(db_path: str) -> str:
    since = datetime.now(timezone.utc) - timedelta(days=7)
    con = duckdb.connect(db_path)
    try:
        w = _weekly_payload(con, since)
    finally:
        con.close()
    return _format_weekly_html(w)


def generate_paper_trade_report(db_path: str, run_id: str | None = None) -> str:
    """
    Markdown report of paper trading activity, optionally limited to a single `run_id`.
    """
    con = duckdb.connect(db_path)
    scope: list[str] = [f"All runs in `{db_path}`"]
    rparams: list = []
    rclause = "1=1"
    if run_id:
        scope = [f"Run `run_id` = `{run_id}`"]
        rclause = "run_id = ?"
        rparams = [run_id]

    pos_rows = con.execute(
        f"""
        SELECT
          run_id, signal_id, contract_id, status,
          net_qty, avg_entry_price, mark_price, unrealized_pnl, realized_pnl, opened_at_utc
        FROM paper_positions
        WHERE {rclause}
        ORDER BY opened_at_utc DESC
        LIMIT 200
        """,
        rparams,
    ).fetchall()

    ord_count = con.execute(
        f"SELECT COUNT(*), COALESCE(SUM(COALESCE(fill_qty,0)),0) FROM paper_orders WHERE {rclause}", rparams
    ).fetchone()
    pnl = con.execute(
        f"""
        SELECT COALESCE(SUM(COALESCE(unrealized_pnl,0) + COALESCE(realized_pnl,0)), 0)
        FROM paper_positions WHERE {rclause}
        """,
        rparams,
    ).fetchone()[0]
    con.close()

    n_orders, _ = (0, 0.0) if ord_count is None else (ord_count[0], ord_count[1])
    lines = [
        "# Paper trading report",
        "",
        f"- {scope[0]}",
        f"- Total paper orders: {n_orders}",
        f"- Total PnL (unreal + realized): {float(pnl):.6f}",
        "",
        "## Positions (up to 200, newest first)",
    ]
    if not pos_rows:
        lines.append("- (none)")
    else:
        for row in pos_rows:
            (rid, sid, cid, st, nq, ep, mp, u, r, oa) = row
            lines.append(
                f"- run={rid!s} status={st} contract={cid} entry={ep} mark={mp} uPnL={u} rPnL={r} opened={oa}"
            )
    return "\n".join(lines)
