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


def _short_text(s: Any, max_len: int = 140) -> str:
    t = "" if s is None else str(s).strip()
    if len(t) <= max_len:
        return t
    return t[: max_len - 1] + "…"


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

    settle_row = con.execute(
        """
        SELECT
          COALESCE(SUM(realized_pnl), 0)::DOUBLE AS sum_r,
          COUNT(*)::BIGINT AS n
        FROM paper_positions
        WHERE status = 'closed' AND realized_pnl IS NOT NULL
        """
    ).fetchone()
    avg_realized = None
    if settle_row and settle_row[1] and int(settle_row[1]) > 0:
        avg_realized = float(settle_row[0]) / float(settle_row[1])

    open_mark = con.execute(
        """
        SELECT COALESCE(SUM(COALESCE(realized_pnl, 0) + COALESCE(unrealized_pnl, 0)), 0)::DOUBLE
        FROM paper_positions
        WHERE status = 'open'
        """
    ).fetchone()[0]
    total_mark = con.execute(
        """
        SELECT COALESCE(SUM(COALESCE(realized_pnl, 0) + COALESCE(unrealized_pnl, 0)), 0)::DOUBLE
        FROM paper_positions
        """
    ).fetchone()[0]
    open_holdings = con.execute(
        """
        SELECT
          p.contract_id,
          p.net_qty,
          p.avg_entry_price,
          p.unrealized_pnl,
          s.thesis_module,
          s.decision,
          s.edge_bps,
          s.model_probability,
          s.market_implied_probability,
          s.decision_reason
        FROM paper_positions AS p
        LEFT JOIN signals AS s ON p.signal_id = s.signal_id
        WHERE p.status = 'open'
        ORDER BY p.opened_at_utc DESC NULLS LAST
        LIMIT 40
        """
    ).fetchall()
    exits_window = con.execute(
        """
        SELECT
          contract_id,
          realized_pnl,
          COALESCE(closed_at_utc, opened_at_utc) AS when_ts,
          run_id
        FROM paper_positions
        WHERE status = 'closed'
          AND COALESCE(closed_at_utc, opened_at_utc) >= ?
        ORDER BY COALESCE(closed_at_utc, opened_at_utc) DESC NULLS LAST
        LIMIT 25
        """,
        [since],
    ).fetchall()
    opens_window = con.execute(
        """
        SELECT
          p.contract_id,
          p.opened_at_utc,
          p.net_qty,
          p.avg_entry_price,
          s.thesis_module,
          s.decision,
          s.edge_bps
        FROM paper_positions AS p
        LEFT JOIN signals AS s ON p.signal_id = s.signal_id
        WHERE p.opened_at_utc >= ?
        ORDER BY p.opened_at_utc DESC NULLS LAST
        LIMIT 25
        """,
        [since],
    ).fetchall()
    recent_fills = con.execute(
        """
        SELECT
          contract_id,
          fill_price,
          fill_qty,
          COALESCE(fill_price * fill_qty, 0)::DOUBLE AS notion
        FROM paper_orders
        WHERE status = 'filled'
          AND COALESCE(filled_at_utc, submitted_at_utc) >= ?
        ORDER BY COALESCE(filled_at_utc, submitted_at_utc) DESC NULLS LAST
        LIMIT 25
        """,
        [since],
    ).fetchall()

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
        "lifetime_realized_sum": float(settle_row[0]) if settle_row else 0.0,
        "lifetime_closed_n": int(settle_row[1]) if settle_row and settle_row[1] is not None else 0,
        "lifetime_avg_realized": avg_realized,
        "open_book_mark": float(open_mark) if open_mark is not None else 0.0,
        "total_mark": float(total_mark) if total_mark is not None else 0.0,
        "open_holdings": open_holdings,
        "exits_window": exits_window,
        "opens_window": opens_window,
        "recent_fills": recent_fills,
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
        "## TL;DR",
        "### Bank (paper PnL)",
        f"- **Settled (sum of realized on closed positions, all time):** {_fmtf(w.get('lifetime_realized_sum'))}",
    ]
    lc = w.get("lifetime_closed_n", 0)
    if lc:
        lar = w.get("lifetime_avg_realized")
        lines.append(f"- **Closed trades (count):** {lc} · **avg realized per close:** {_fmtf(lar)}")
    else:
        lines.append("- **Closed trades (count):** 0")
    lines.extend(
        [
            f"- **Open positions (mark — sum of real+unreal on opens):** {_fmtf(w.get('open_book_mark'))}",
            f"- **All rows (total mark — unreal + realized everywhere):** {_fmtf(w.get('total_mark'))}",
            "",
            "### Positions open now (with last signal context)",
        ]
    )
    oh = w.get("open_holdings") or []
    if not oh:
        lines.append("- _(none)_")
    else:
        for row in oh:
            (
                cid,
                nq,
                ep,
                upnl,
                thesis,
                decision,
                eb,
                mp,
                mip,
                reason,
            ) = row
            thesis_s = thesis or "—"
            dec = decision or "—"
            lines.append(
                f"- **{cid}** · thesis `{thesis_s}` · `{dec}` · edge {_fmtf(eb)} bps · "
                f"model P {_fmtf(mp)} vs mkt {_fmtf(mip)} · uPnL {_fmtf(upnl)} · "
                f"_Why:_ {_short_text(reason, 160)}"
            )

    lines.extend(["", f"### Position exits (closed in this {7}-day window)"])
    ew = w.get("exits_window") or []
    if not ew:
        lines.append("- _(none)_")
    else:
        for cid, rp, wts, rid in ew:
            lines.append(f"- **{cid}** · realized {_fmtf(rp)} · closed `{wts}` · run `{rid}`")

    lines.extend(["", "### New opens (positions opened in this window)"])
    ow = w.get("opens_window") or []
    if not ow:
        lines.append("- _(none)_")
    else:
        for cid, oa, nq, apr, thesis, decision, eb in ow:
            apr_f = float(apr) if apr is not None else 0.0
            nqf = float(nq) if nq is not None else 0.0
            notion = abs(nqf * apr_f)
            th = thesis or "—"
            dc = decision or "—"
            lines.append(
                f"- **{cid}** · opened `{oa}` · qty {_fmtf(nqf)} @ {_fmtf(apr_f)} "
                f"(~notional {_fmtf(notion)}) · `{th}` / `{dc}` · edge {_fmtf(eb)} bps"
            )

    lines.extend(["", "### Recent filled orders (same window, settlement detail)"])
    rf = w.get("recent_fills") or []
    if not rf:
        lines.append("- _(none)_")
    else:
        for cid, fp, fq, notion in rf:
            lines.append(f"- **{cid}** · fill {_fmtf(fp)} × qty {_fmtf(fq)} ≈ notional {_fmtf(notion)}")

    lines.extend(
        [
            "",
            "_Deeper stats and signal cohorts follow._",
            "",
            f"- **Window (UTC):** since `{w['since'].isoformat()}`",
            "",
            "## Signals",
            f"- Total signals: {w['signals_n']}",
            f"- Distinct contracts evaluated: {w['contracts_n']}",
            "",
            "### By thesis",
        ]
    )
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

    lc = int(w.get("lifetime_closed_n") or 0)
    lar = w.get("lifetime_avg_realized")
    avg_line = (
        f"<p>Closed trades: {lc} · avg realized/close: {_escape(_fmtf(lar))}</p>" if lc else "<p>Closed trades: 0</p>"
    )
    oh = w.get("open_holdings") or []
    rows_open = "".join(
        f"<tr><td>{_escape(r[0])}</td><td>{_escape(r[4])}</td><td>{_escape(r[5])}</td>"
        f"<td>{_escape(_fmtf(r[6]))}</td><td>{_escape(_fmtf(r[7]))}</td><td>{_escape(_fmtf(r[8]))}</td>"
        f"<td>{_escape(_fmtf(r[3]))}</td><td>{_escape(_short_text(r[9], 200))}</td></tr>"
        for r in oh
    )
    ew = w.get("exits_window") or []
    rows_ex = "".join(
        f"<tr><td>{_escape(c)}</td><td>{_escape(_fmtf(rp))}</td><td>{_escape(str(wts))}</td>"
        f"<td>{_escape(str(rid))}</td></tr>"
        for c, rp, wts, rid in ew
    )
    osw = w.get("opens_window") or []
    rows_nw = "".join(
        f"<tr><td>{_escape(c)}</td><td>{_escape(str(oa))}</td><td>{_escape(_fmtf(nq))}</td>"
        f"<td>{_escape(_fmtf(ap))}</td><td>{_escape(th or '')}</td><td>{_escape(dc or '')}</td>"
        f"<td>{_escape(_fmtf(eb))}</td></tr>"
        for c, oa, nq, ap, th, dc, eb in osw
    )
    rf = w.get("recent_fills") or []
    rows_f = "".join(
        f"<tr><td>{_escape(c)}</td><td>{_escape(_fmtf(px))}</td><td>{_escape(_fmtf(fq))}</td>"
        f"<td>{_escape(_fmtf(notion))}</td></tr>"
        for c, px, fq, notion in rf
    )

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"/><title>Weekly digest</title>
<style>body{{font-family:system-ui,sans-serif;max-width:960px;margin:1rem auto;}}
table{{border-collapse:collapse;width:100%;margin:0.5rem 0}}th,td{{border:1px solid #ccc;padding:0.35rem 0.5rem;text-align:left;font-size:0.92rem}}
th{{background:#f4f4f4}}</style>
</head><body>
<h1>Weekly Prediction Market Edge digest</h1>
<h2>TL;DR</h2>
<h3>Bank (paper)</h3>
<p>Settled realized (lifetime sum on closed legs): {_escape(_fmtf(w.get('lifetime_realized_sum')))}</p>
{avg_line}
<p>Open book mark (sum real+unreal on open legs): {_escape(_fmtf(w.get('open_book_mark')))}</p>
<p>Total mark across all legs: {_escape(_fmtf(w.get('total_mark')))}</p>
<h3>Positions open now</h3>
<table><thead><tr><th>Contract</th><th>Thesis</th><th>Decision</th><th>Edge bps</th><th>Model P</th><th>Mkt P</th><th>Unrealized</th><th>Rationale</th></tr></thead>
<tbody>{rows_open or '<tr><td colspan="8">(none)</td></tr>'}</tbody></table>
<h3>Exits (7-day window)</h3>
<table><thead><tr><th>Contract</th><th>Realized</th><th>When</th><th>Run</th></tr></thead>
<tbody>{rows_ex or '<tr><td colspan="4">(none)</td></tr>'}</tbody></table>
<h3>New opens (window)</h3>
<table><thead><tr><th>Contract</th><th>Opened</th><th>Qty</th><th>Entry</th><th>Thesis</th><th>Decision</th><th>Edge</th></tr></thead>
<tbody>{rows_nw or '<tr><td colspan="7">(none)</td></tr>'}</tbody></table>
<h3>Recent fills</h3>
<table><thead><tr><th>Contract</th><th>Fill</th><th>Qty</th><th>≈ Notional</th></tr></thead>
<tbody>{rows_f or '<tr><td colspan="4">(none)</td></tr>'}</tbody></table>

<p><em>Deeper cohort stats below · Window:</em> <code>{_escape(w['since'].isoformat())}</code></p>

<h2>Signals</h2>
<p>Total: {w['signals_n']} · Distinct contracts: {w['contracts_n']}</p>
<p>Edge bps — mean {_escape(_fmtf(w['edge_avg']))}, std {_escape(_fmtf(w['edge_std']))}, min {_escape(_fmtf(w['edge_min']))}, max {_escape(_fmtf(w['edge_max']))}</p>
<h3>By thesis</h3>
<table><thead><tr><th>Thesis</th><th>Signals</th></tr></thead><tbody>{rows_sig or '<tr><td colspan="2">(none)</td></tr>'}</tbody></table>
<h3>Decisions</h3>
<table><thead><tr><th>Decision</th><th>Count</th></tr></thead><tbody>{rows_dec or '<tr><td colspan="2">(none)</td></tr>'}</tbody></table>
<h2>Paper trading (cohort)</h2>
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
<p><em>Persistence: DuckDB plus workflow artifact <strong>pme-state</strong>.</em></p>
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
