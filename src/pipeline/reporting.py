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
        WHERE status = 'closed' AND realized_pnl IS NOT NULL AND realized_pnl > 0
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
          AND COALESCE(p.close_reason, '') != 'dedup_consolidated'
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


def _fmtp(x: Any, decimals: int = 2) -> str:
    """Format a price or PnL with sign prefix; zero shows without sign."""
    if x is None:
        return "—"
    try:
        v = float(x)
        if v == 0:
            return f"0.{'0' * decimals}"
        return f"{v:+.{decimals}f}"
    except (TypeError, ValueError):
        return str(x)


def _fmtbps(x: Any) -> str:
    """Format edge bps as a whole integer with comma separator."""
    if x is None:
        return "—"
    try:
        return f"{int(round(float(x))):,}"
    except (TypeError, ValueError):
        return str(x)


def _fmt_ts(ts: Any, fmt: str = "%b %d %H:%M UTC") -> str:
    """Format a datetime or ISO string to a compact human-readable form."""
    if ts is None:
        return "—"
    if isinstance(ts, datetime):
        return ts.strftime(fmt)
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        return dt.strftime(fmt)
    except (ValueError, AttributeError):
        return str(ts)


def _parse_reason(reason: Any) -> str:
    """Parse semicolon-delimited key=value reason string into readable text."""
    if not reason:
        return ""
    s = str(reason).strip()
    pairs = [p.strip() for p in s.split(";") if "=" in p]
    if not pairs:
        return s
    parts = []
    for pair in pairs:
        k, _, v = pair.partition("=")
        k = k.strip().replace("_", " ")
        v = v.strip()
        try:
            fv = float(v)
            if abs(fv) >= 1000:
                v = f"{fv:,.0f}"
            elif fv == 0:
                v = "0"
            elif abs(fv) < 0.0001:
                v = f"{fv:.2e}"
            else:
                v = f"{fv:.4f}".rstrip("0").rstrip(".")
        except ValueError:
            pass
        parts.append(f"{k}: {v}")
    return " · ".join(parts)


def _format_weekly_md(w: dict[str, Any]) -> str:
    since: datetime = w["since"]
    now = datetime.now(timezone.utc)
    since_str = since.strftime("%b %d")
    now_str = now.strftime("%b %d, %Y")
    runs_n = w.get("runs_n", 0) or 0
    signals_n = w.get("signals_n", 0) or 0

    lines = [
        "# Weekly Edge Digest",
        f"**{since_str} – {now_str}  ·  {runs_n} pipeline run{'s' if runs_n != 1 else ''}  ·  {signals_n} signal{'s' if signals_n != 1 else ''} evaluated**",
        "",
        "---",
        "",
    ]

    # Overview table
    lifetime_sum = w.get("lifetime_realized_sum") or 0.0
    lc = w.get("lifetime_closed_n", 0) or 0
    open_mark = w.get("open_book_mark") or 0.0
    order_count = w.get("order_count", 0) or 0
    open_pos = w.get("open_positions", 0) or 0

    lines.extend(
        [
            "## Overview",
            "",
            "| Metric | Value |",
            "|--------|-------|",
            f"| Lifetime realized PnL | {_fmtp(lifetime_sum)} ({lc} closed trade{'s' if lc != 1 else ''}) |",
            f"| Open book mark | {_fmtp(open_mark)} |",
            f"| Orders placed this week | {order_count} |",
            f"| Open positions | {open_pos} |",
            "",
            "---",
            "",
        ]
    )

    # Open positions — each gets its own sub-section
    oh = w.get("open_holdings") or []
    lines.extend([f"## Open Positions ({len(oh)})", ""])
    if not oh:
        lines.append("_No open positions._")
    else:
        for row in oh:
            cid, nq, ep, upnl, thesis, decision, eb, mp, mip, reason = row
            nqf = float(nq) if nq is not None else 0.0
            epf = float(ep) if ep is not None else 0.0
            notion = abs(nqf * epf)
            mpf = float(mp) * 100 if mp is not None else None
            mipf = float(mip) * 100 if mip is not None else None

            lines.append(f"### {cid}")
            lines.append(f"- **Decision:** `{decision or '—'}`")
            lines.append(f"- **Entry:** {epf:.4f} × {nqf:.2f} shares ≈ ${notion:.2f} notional")
            lines.append(f"- **Unrealized PnL:** {_fmtp(upnl)}")
            if mpf is not None and mipf is not None:
                lines.append(f"- **Model vs Market:** {mpf:.1f}% vs {mipf:.1f}%  (edge: {_fmtbps(eb)} bps)")
            lines.append(f"- **Thesis:** `{thesis or '—'}`")
            if reason:
                lines.append(f"- **Signal factors:** {_parse_reason(reason)}")
            lines.append("")

    lines.extend(["---", ""])

    # Activity this week
    ew = w.get("exits_window") or []
    ow = w.get("opens_window") or []
    rf = w.get("recent_fills") or []

    lines.extend(
        [
            "## Activity This Week",
            "",
            f"**New positions opened:** {len(ow)}  ",
            f"**Positions closed:** {len(ew)}" + ("" if ew else " (none)"),
            "",
        ]
    )

    if ow:
        lines.extend(
            [
                "### New Opens",
                "",
                "| Contract | Opened | Qty @ Entry | ≈ Notional | Thesis | Decision | Edge (bps) |",
                "|----------|--------|-------------|------------|--------|----------|------------|",
            ]
        )
        for cid, oa, nq, apr, thesis, decision, eb in ow:
            apr_f = float(apr) if apr is not None else 0.0
            nqf = float(nq) if nq is not None else 0.0
            notion = abs(nqf * apr_f)
            lines.append(
                f"| {cid} | {_fmt_ts(oa, '%b %d %H:%M')} | {nqf:.2f} @ {apr_f:.4f}"
                f" | ${notion:.2f} | {thesis or '—'} | {decision or '—'} | {_fmtbps(eb)} |"
            )
        lines.append("")

    if ew:
        lines.extend(
            [
                "### Exits",
                "",
                "| Contract | Realized PnL | Closed |",
                "|----------|-------------|--------|",
            ]
        )
        for cid, rp, wts, _rid in ew:
            lines.append(f"| {cid} | {_fmtp(rp)} | {_fmt_ts(wts, '%b %d %H:%M')} |")
        lines.append("")

    if rf:
        lines.extend(
            [
                "### Fills",
                "",
                "| Contract | Fill Price | Qty | ≈ Notional |",
                "|----------|-----------|-----|------------|",
            ]
        )
        for cid, fp, fq, notion in rf:
            lines.append(f"| {cid} | {float(fp):.4f} | {float(fq):.2f} | ${float(notion):.2f} |")
        lines.append("")

    if not ow and not ew and not rf:
        lines.extend(["_No orders or position changes this week._", ""])

    lines.extend(["---", ""])

    # Signal summary — compact
    lines.extend(["## Signal Summary", ""])
    lines.append(
        f"**{signals_n} signal{'s' if signals_n != 1 else ''} across "
        f"{w.get('contracts_n', 0)} distinct contract{'s' if (w.get('contracts_n') or 0) != 1 else ''}**"
    )
    lines.append("")

    if w.get("signal_by_thesis"):
        lines.extend(["| Thesis | Signals |", "|--------|---------|"])
        for thesis, n in w["signal_by_thesis"]:
            lines.append(f"| {thesis} | {n} |")
        lines.append("")

    if w.get("decisions"):
        dec_parts = [f"`{d}`: {n}" for d, n in w["decisions"]]
        lines.append(f"**Decisions:** {' · '.join(dec_parts)}")
        lines.append("")

    edge_avg = w.get("edge_avg")
    edge_std = w.get("edge_std")
    edge_min = w.get("edge_min")
    edge_max = w.get("edge_max")
    edge_parts = [f"mean {_fmtbps(edge_avg)} bps"]
    if edge_std is not None:
        edge_parts.append(f"std {_fmtbps(edge_std)}")
    edge_parts.extend([f"min {_fmtbps(edge_min)}", f"max {_fmtbps(edge_max)}"])
    lines.extend([f"**Edge:** {' · '.join(edge_parts)}", ""])

    if w.get("hit_rate") is not None:
        lines.extend(
            [
                f"**Hit rate** (realized > 0): {w['hit_rate']:.1f}% over {w['closed_n']} closed position{'s' if w['closed_n'] != 1 else ''}",
                "",
            ]
        )

    lines.extend(["**Top winners (window):**", ""])
    if w.get("winners"):
        for cid, rp, _rid in w["winners"]:
            lines.append(f"- {cid}: {_fmtp(rp)}")
    else:
        lines.append("- _(no profitable closed positions this period)_")
    lines.append("")

    if w.get("losers"):
        lines.extend(["**Top losers (window):**", ""])
        for cid, rp, _rid in w["losers"]:
            lines.append(f"- {cid}: {_fmtp(rp)}")
        lines.append("")

    lines.extend(["---", ""])

    # Data freshness — compact footer
    lines.extend(
        [
            "## Data Freshness",
            "",
            f"- **Latest signal:** {_fmt_ts(w.get('last_sig'))}",
            f"- **Pipeline runs this week:** {runs_n}",
            f"- **Last run sources:** {w.get('last_sources') or '—'}",
            "",
            "_Full history: restore `pme-state` artifact from CI · per-connector detail in CI run logs._",
        ]
    )
    return "\n".join(lines)


def _pnl_color(x: Any) -> str:
    """Return CSS color class name based on PnL sign."""
    try:
        return "pos" if float(x) > 0 else ("neg" if float(x) < 0 else "")
    except (TypeError, ValueError):
        return ""


def _format_weekly_html(w: dict[str, Any]) -> str:
    since: datetime = w["since"]
    now = datetime.now(timezone.utc)
    since_str = since.strftime("%b %d")
    now_str = now.strftime("%b %d, %Y")
    runs_n = w.get("runs_n", 0) or 0
    signals_n = w.get("signals_n", 0) or 0

    lifetime_sum = w.get("lifetime_realized_sum") or 0.0
    lc = int(w.get("lifetime_closed_n") or 0)
    open_mark = w.get("open_book_mark") or 0.0
    order_count = w.get("order_count", 0) or 0
    open_pos = w.get("open_positions", 0) or 0

    # Overview stats
    def stat(label: str, value: str, tone: str = "") -> str:
        cls = f' class="{tone}"' if tone else ""
        return f'<div class="stat"><div class="stat-label">{_escape(label)}</div><div class="stat-value{" " + tone if tone else ""}">{value}</div></div>'

    stats_html = "".join(
        [
            stat("Lifetime Realized PnL", f'<span class="{_pnl_color(lifetime_sum)}">{_escape(_fmtp(lifetime_sum))}</span> <small>({lc} closed)</small>'),
            stat("Open Book Mark", f'<span class="{_pnl_color(open_mark)}">{_escape(_fmtp(open_mark))}</span>'),
            stat("Orders This Week", str(order_count)),
            stat("Open Positions", str(open_pos)),
        ]
    )

    # Open holdings
    oh = w.get("open_holdings") or []

    def holding_card(row: tuple) -> str:  # type: ignore[type-arg]
        cid, nq, ep, upnl, thesis, decision, eb, mp, mip, reason = row
        nqf = float(nq) if nq is not None else 0.0
        epf = float(ep) if ep is not None else 0.0
        notion = abs(nqf * epf)
        mpf = float(mp) * 100 if mp is not None else None
        mipf = float(mip) * 100 if mip is not None else None
        model_mkt = (
            f'<tr><td>Model vs Market</td><td>{mpf:.1f}% vs {mipf:.1f}%'
            f' <span class="muted">(edge: {_escape(_fmtbps(eb))} bps)</span></td></tr>'
            if mpf is not None and mipf is not None
            else ""
        )
        reason_row = (
            f'<tr><td>Signal factors</td><td class="muted small">{_escape(_parse_reason(reason))}</td></tr>'
            if reason
            else ""
        )
        upnl_cls = _pnl_color(upnl)
        return (
            f'<div class="holding">'
            f'<div class="holding-title">{_escape(str(cid))}</div>'
            f'<table class="kv"><tbody>'
            f'<tr><td>Decision</td><td><code>{_escape(str(decision or "—"))}</code></td></tr>'
            f'<tr><td>Entry</td><td>{epf:.4f} × {nqf:.2f} shares ≈ <strong>${notion:.2f}</strong> notional</td></tr>'
            f'<tr><td>Unrealized PnL</td><td><span class="{upnl_cls}">{_escape(_fmtp(upnl))}</span></td></tr>'
            f'{model_mkt}'
            f'<tr><td>Thesis</td><td><code>{_escape(str(thesis or "—"))}</code></td></tr>'
            f'{reason_row}'
            f'</tbody></table></div>'
        )

    holdings_html = "".join(holding_card(r) for r in oh) if oh else "<p class='muted'>No open positions.</p>"

    # New opens table
    osw = w.get("opens_window") or []
    rows_nw = "".join(
        f"<tr><td>{_escape(str(c))}</td><td>{_escape(_fmt_ts(oa, '%b %d %H:%M'))}</td>"
        f"<td>{float(nq):.2f} @ {float(ap):.4f}</td><td>${abs(float(nq or 0) * float(ap or 0)):.2f}</td>"
        f"<td>{_escape(str(th or '—'))}</td><td><code>{_escape(str(dc or '—'))}</code></td>"
        f"<td>{_escape(_fmtbps(eb))}</td></tr>"
        for c, oa, nq, ap, th, dc, eb in osw
    )

    # Exits table
    ew = w.get("exits_window") or []
    rows_ex = "".join(
        f"<tr><td>{_escape(str(c))}</td>"
        f'<td><span class="{_pnl_color(rp)}">{_escape(_fmtp(rp))}</span></td>'
        f"<td>{_escape(_fmt_ts(wts, '%b %d %H:%M'))}</td></tr>"
        for c, rp, wts, _rid in ew
    )

    # Fills table
    rf = w.get("recent_fills") or []
    rows_f = "".join(
        f"<tr><td>{_escape(str(c))}</td><td>{float(px):.4f}</td><td>{float(fq):.2f}</td><td>${float(notion):.2f}</td></tr>"
        for c, px, fq, notion in rf
    )

    # Signal cohort
    rows_sig = "".join(f"<tr><td>{_escape(str(a))}</td><td>{n}</td></tr>" for a, n in w["signal_by_thesis"])
    rows_dec = "".join(f"<tr><td><code>{_escape(str(a))}</code></td><td>{n}</td></tr>" for a, n in w["decisions"])

    hit = f"{w['hit_rate']:.1f}%" if w["hit_rate"] is not None else "n/a"

    win_rows = "".join(
        f'<tr><td>{_escape(str(c))}</td><td><span class="pos">{_escape(_fmtp(rp))}</span></td></tr>'
        for c, rp, _r in w["winners"]
    )
    lose_rows = "".join(
        f'<tr><td>{_escape(str(c))}</td><td><span class="neg">{_escape(_fmtp(rp))}</span></td></tr>'
        for c, rp, _r in w["losers"]
    )

    edge_parts = [f"mean {_escape(_fmtbps(w['edge_avg']))} bps"]
    if w.get("edge_std") is not None:
        edge_parts.append(f"std {_escape(_fmtbps(w['edge_std']))}")
    edge_parts.extend([f"min {_escape(_fmtbps(w['edge_min']))}", f"max {_escape(_fmtbps(w['edge_max']))}"])

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"/>
<title>Weekly Edge Digest · {_escape(since_str)}–{_escape(now_str)}</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:system-ui,-apple-system,sans-serif;font-size:0.92rem;line-height:1.5;
  color:#1a1a1a;background:#fff;max-width:900px;margin:2rem auto;padding:0 1.25rem}}
h1{{font-size:1.5rem;font-weight:700;margin-bottom:0.2rem}}
h2{{font-size:1.1rem;font-weight:600;margin:2rem 0 0.75rem;border-bottom:1px solid #e5e5e5;padding-bottom:0.3rem}}
h3{{font-size:0.95rem;font-weight:600;margin:1.25rem 0 0.5rem;color:#444}}
.subtitle{{color:#666;font-size:0.88rem;margin-bottom:1.5rem}}
hr{{border:none;border-top:1px solid #e5e5e5;margin:1.5rem 0}}
.stats{{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:1rem;margin:1rem 0 1.5rem}}
.stat{{background:#f7f7f7;border-radius:6px;padding:0.75rem 1rem}}
.stat-label{{font-size:0.78rem;color:#666;text-transform:uppercase;letter-spacing:0.04em;margin-bottom:0.2rem}}
.stat-value{{font-size:1.15rem;font-weight:600}}
.holdings{{display:grid;gap:1rem;margin:0.75rem 0}}
.holding{{border:1px solid #e5e5e5;border-radius:6px;padding:1rem}}
.holding-title{{font-weight:600;font-size:1rem;margin-bottom:0.6rem}}
table{{border-collapse:collapse;width:100%;margin:0.5rem 0}}
th,td{{padding:0.35rem 0.65rem;text-align:left;border-bottom:1px solid #ebebeb;font-size:0.88rem}}
th{{background:#f7f7f7;font-weight:600;color:#444}}
table.kv td:first-child{{width:160px;color:#666;font-size:0.82rem;padding-left:0}}
table.kv td{{border:none;padding:0.2rem 0.5rem 0.2rem 0}}
.pos{{color:#16a34a;font-weight:500}}
.neg{{color:#dc2626;font-weight:500}}
.muted{{color:#666}}
.small{{font-size:0.82rem}}
code{{background:#f3f3f3;padding:0.1em 0.35em;border-radius:3px;font-size:0.85rem}}
small{{font-size:0.82rem;font-weight:400;color:#555}}
</style>
</head><body>

<h1>Weekly Edge Digest</h1>
<p class="subtitle">{_escape(since_str)} – {_escape(now_str)} &nbsp;·&nbsp; {runs_n} pipeline run{'s' if runs_n != 1 else ''} &nbsp;·&nbsp; {signals_n} signal{'s' if signals_n != 1 else ''} evaluated</p>

<h2>Overview</h2>
<div class="stats">{stats_html}</div>

<hr/>

<h2>Open Positions ({len(oh)})</h2>
<div class="holdings">{holdings_html}</div>

<hr/>

<h2>Activity This Week</h2>
<p><strong>New positions opened:</strong> {len(osw)} &nbsp; <strong>Positions closed:</strong> {len(ew) if ew else "0 (none)"}</p>

{'<h3>New Opens</h3><table><thead><tr><th>Contract</th><th>Opened</th><th>Qty @ Entry</th><th>≈ Notional</th><th>Thesis</th><th>Decision</th><th>Edge (bps)</th></tr></thead><tbody>' + (rows_nw or '<tr><td colspan="7" class="muted">(none)</td></tr>') + '</tbody></table>' if osw else ''}
{'<h3>Exits</h3><table><thead><tr><th>Contract</th><th>Realized PnL</th><th>Closed</th></tr></thead><tbody>' + (rows_ex or '<tr><td colspan="3" class="muted">(none)</td></tr>') + '</tbody></table>' if ew else ''}
{'<h3>Fills</h3><table><thead><tr><th>Contract</th><th>Fill Price</th><th>Qty</th><th>≈ Notional</th></tr></thead><tbody>' + (rows_f or '<tr><td colspan="4" class="muted">(none)</td></tr>') + '</tbody></table>' if rf else ''}

<hr/>

<h2>Signal Summary</h2>
<p><strong>{signals_n} signal{'s' if signals_n != 1 else ''}</strong> across {w.get('contracts_n', 0)} distinct contract{'s' if (w.get('contracts_n') or 0) != 1 else ''}</p>
<p class="muted small" style="margin-top:0.4rem">Edge: {" · ".join(edge_parts)}</p>

<div style="display:grid;grid-template-columns:1fr 1fr;gap:1.5rem;margin-top:1rem">
<div>
<h3>By Thesis</h3>
<table><thead><tr><th>Thesis</th><th>Signals</th></tr></thead>
<tbody>{rows_sig or '<tr><td colspan="2" class="muted">(none)</td></tr>'}</tbody></table>
</div>
<div>
<h3>Decisions</h3>
<table><thead><tr><th>Decision</th><th>Count</th></tr></thead>
<tbody>{rows_dec or '<tr><td colspan="2" class="muted">(none)</td></tr>'}</tbody></table>
</div>
</div>

{'<p style="margin-top:1rem"><strong>Hit rate</strong> (realized &gt; 0): ' + hit + ' over ' + str(w["closed_n"]) + ' closed position' + ('s' if w['closed_n'] != 1 else '') + '</p>' if w['hit_rate'] is not None else ''}

{'<div style="display:grid;grid-template-columns:1fr 1fr;gap:1.5rem;margin-top:1rem"><div><h3>Top Winners</h3><table><thead><tr><th>Contract</th><th>Realized PnL</th></tr></thead><tbody>' + (win_rows or '<tr><td colspan="2" class="muted">(none)</td></tr>') + '</tbody></table></div><div><h3>Top Losers</h3><table><thead><tr><th>Contract</th><th>Realized PnL</th></tr></thead><tbody>' + (lose_rows or '<tr><td colspan="2" class="muted">(none)</td></tr>') + '</tbody></table></div></div>' if w.get('winners') or w.get('losers') else ''}

<hr/>

<h2>Data Freshness</h2>
<p>Latest signal: <strong>{_escape(_fmt_ts(w.get('last_sig')))}</strong> &nbsp;·&nbsp; Pipeline runs this week: {runs_n}</p>
<p class="muted small" style="margin-top:0.25rem">Sources: {_escape(str(w.get('last_sources') or '—'))}</p>
<p class="muted small" style="margin-top:1rem"><em>Full history: restore <code>pme-state</code> artifact from CI · per-connector detail in CI run logs.</em></p>

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
