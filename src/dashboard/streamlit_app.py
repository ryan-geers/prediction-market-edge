"""
Local Streamlit dashboard: signals, forecasts vs market, paper PnL, and run health.
Run from repo root: streamlit run src/dashboard/streamlit_app.py
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone

import duckdb
import pandas as pd
import streamlit as st

from src.core.config import get_settings

UTC = timezone.utc


st.set_page_config(page_title="Prediction Market Edge", layout="wide", initial_sidebar_state="expanded")

settings = get_settings()
DB = str(settings.duckdb_path)


def _con_readonly() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DB, read_only=True)


@st.cache_data(ttl=30)
def _distinct_thesis_venue() -> tuple[list[str], list[str]]:
    if not settings.duckdb_path.is_file():
        return [], []
    con = _con_readonly()
    try:
        t_sig = con.execute("SELECT DISTINCT thesis_module FROM signals WHERE thesis_module IS NOT NULL").fetchall()
        t_mf = con.execute("SELECT DISTINCT thesis_module FROM model_forecasts WHERE thesis_module IS NOT NULL").fetchall()
        theses = sorted({r[0] for r in t_sig + t_mf if r[0]})
        v = con.execute("SELECT DISTINCT venue FROM signals WHERE venue IS NOT NULL").fetchall()
        venues = sorted({r[0] for r in v if r[0]})
    finally:
        con.close()
    return theses, venues


st.title("Prediction Market Edge")
st.caption("Local, on-demand inspection of persisted pipeline state (DuckDB). v1: leave thesis/venue unselected to include all.")

if not settings.duckdb_path.is_file():
    st.warning(f"No database at `{DB}`. Run `pme run` (or restore a GitHub Actions artifact) first.")
    st.stop()

thesis_options, venue_options = _distinct_thesis_venue()

default_start = date.today() - timedelta(days=30)
default_end = date.today()

with st.sidebar:
    st.subheader("Filters")
    filter_thesis = st.multiselect(
        "Thesis (empty = all)",
        options=thesis_options,
        default=[],
    )
    filter_venue = st.multiselect(
        "Venue (empty = all)",
        options=venue_options,
        default=[],
    )
    contract_q = st.text_input("Contract ID contains", value="", help="Case-insensitive substring match.")
    d_range = st.date_input(
        "Event window (UTC): signals; paper uses opened_at in same range",
        value=(default_start, default_end),
    )
    if isinstance(d_range, (list, tuple)) and len(d_range) == 2:
        ds, de = d_range[0], d_range[1]
    else:
        ds = de = d_range if not isinstance(d_range, (list, tuple)) else d_range[0]

t0 = datetime.combine(ds, time.min, tzinfo=UTC)
t1 = datetime.combine(de, time(23, 59, 59, 999999), tzinfo=UTC)


def build_signal_where(table_alias: str) -> tuple[str, list]:
    """Time + optional thesis, venue, contract (requires event_time_utc and thesis/venue on table)."""
    parts: list[str] = [f"{table_alias}.event_time_utc >= ?", f"{table_alias}.event_time_utc <= ?"]
    params: list = [t0, t1]
    if filter_thesis:
        ph = ", ".join(["?"] * len(filter_thesis))
        parts.append(f"{table_alias}.thesis_module IN ({ph})")
        params.extend(filter_thesis)
    if filter_venue:
        ph = ", ".join(["?"] * len(filter_venue))
        parts.append(f"{table_alias}.venue IN ({ph})")
        params.extend(filter_venue)
    if contract_q.strip():
        parts.append(f"LOWER({table_alias}.contract_id) LIKE LOWER(?)")
        params.append(f"%{contract_q.strip()}%")
    return " AND ".join(parts), params


def build_paper_where() -> tuple[str, list]:
    """Same date window on opened_at; optional venue, thesis (via run_id), contract id."""
    parts: list[str] = ["p.opened_at_utc >= ?", "p.opened_at_utc <= ?"]
    params: list = [t0, t1]
    if filter_venue:
        ph = ", ".join(["?"] * len(filter_venue))
        parts.append(f"p.venue IN ({ph})")
        params.extend(filter_venue)
    if filter_thesis:
        ph = ", ".join(["?"] * len(filter_thesis))
        parts.append(
            f"p.run_id IN (SELECT DISTINCT run_id FROM signals WHERE thesis_module IN ({ph}))"
        )
        params.extend(filter_thesis)
    if contract_q.strip():
        parts.append("LOWER(p.contract_id) LIKE LOWER(?)")
        params.append(f"%{contract_q.strip()}%")
    return " AND ".join(parts), params


con = _con_readonly()

# --- Overview: health & freshness
last_run = con.execute(
    """
    SELECT run_id, active_thesis, started_at_utc, completed_at_utc, data_sources, code_commit_sha
    FROM run_manifest
    ORDER BY COALESCE(completed_at_utc, started_at_utc) DESC NULLS LAST
    LIMIT 1
    """
).df()
max_sig = con.execute("SELECT MAX(event_time_utc) AS t FROM signals").fetchone()
max_fc = con.execute("SELECT MAX(created_at_utc) AS t FROM model_forecasts").fetchone()
cnt_7d = con.execute(
    "SELECT COUNT(*) FROM signals WHERE event_time_utc >= ?",
    [datetime.now(UTC) - timedelta(days=7)],
).fetchone()

tab_over, tab_sig, tab_fc, tab_paper = st.tabs(["Overview", "Signals & edge", "Forecasts vs market", "Paper trading"])

with tab_over:
    st.subheader("Run and data freshness")
    c1, c2, c3 = st.columns(3)
    if not last_run.empty:
        done = last_run["completed_at_utc"].iloc[0]
        c1.metric("Last run completed (UTC)", str(done)[:19] if done is not None else "—")
        c2.metric("Last run thesis", str(last_run["active_thesis"].iloc[0]))
    else:
        c1.info("No `run_manifest` rows yet.")
        c2.caption("—")
    c3.metric("Signals in last 7d", int(cnt_7d[0] if cnt_7d else 0))
    t_sig_s = str(max_sig[0])[:19] if max_sig and max_sig[0] else "—"
    t_fc = str(max_fc[0])[:19] if max_fc and max_fc[0] else "—"
    st.caption(f"Newest signal event: **{t_sig_s}** — Newest forecast created: **{t_fc}**")
    st.markdown(
        "**Ingest and connectors:** `run_manifest.data_sources` lists the sources for the last run. "
        "Per-connector latency, retries, and error counts are not in DuckDB in v1—check job logs; Phase 6 reporting can add them."
    )
    if not last_run.empty:
        with st.expander("Latest run details"):
            st.dataframe(last_run, use_container_width=True)

# --- Signals
where_s, params_s = build_signal_where("s")
sig_sql = f"""
SELECT
  s.event_time_utc, s.run_id, s.thesis_module, s.venue, s.contract_id, s.contract_label,
  s.model_probability, s.market_implied_probability, s.edge_bps, s.spread_bps,
  s.decision, s.decision_reason
FROM signals s
WHERE {where_s}
ORDER BY s.event_time_utc DESC
LIMIT 500
"""
signals = con.execute(sig_sql, params_s).df()

with tab_sig:
    st.subheader("Signals and expected edge")
    if signals.empty:
        st.info("No signals in the selected range or filters. Widen the date range or clear filters.")
    else:
        st.caption(f"{len(signals)} rows (max 500, newest first).")
        n_ent = (signals["decision"].str.startswith("enter", na=False)).sum()
        r1, r2, r3, r4 = st.columns(4)
        r1.metric("Rows", len(signals))
        r2.metric("Decisions to enter (long y/n)", int(n_ent))
        r3.metric("Edge bps (mean)", f"{signals['edge_bps'].mean():.1f}")
        r4.metric("Abs edge bps (mean)", f"{signals['edge_bps'].abs().mean():.1f}")
        st.dataframe(signals, use_container_width=True, height=400)
        st.subheader("Edge (bps), first 80 rows")
        st.bar_chart(signals["edge_bps"].head(80))

# --- Forecasts vs market
where_join, p_join = build_signal_where("s")
fc_sql = f"""
SELECT
  s.event_time_utc, s.thesis_module, s.venue, s.contract_id, s.contract_label,
  s.model_probability, s.market_implied_probability,
  (s.model_probability - s.market_implied_probability) * 10000 AS model_minus_mid_bps,
  s.edge_bps, s.spread_bps,
  mf.target_metric, mf.validation_rmse, mf.validation_mae, mf.model_version, mf.run_id
FROM signals s
LEFT JOIN model_forecasts mf
  ON mf.run_id = s.run_id AND mf.thesis_module = s.thesis_module
WHERE {where_join}
ORDER BY s.event_time_utc DESC
LIMIT 300
"""
forecasts = con.execute(fc_sql, p_join).df()

with tab_fc:
    st.subheader("Model probability vs market mid")
    st.caption("Same signal row with optional `model_forecasts` for that run. Left join may leave forecast columns null if missing.")
    if forecasts.empty:
        st.info("No rows in the current filters. Widen the window or run `pme run`.")
    else:
        st.dataframe(forecasts, use_container_width=True, height=420)
        if len(forecasts) > 1:
            cmp = pd.DataFrame(
                {
                    "model_p": pd.to_numeric(forecasts["model_probability"], errors="coerce"),
                    "market_mid_p": pd.to_numeric(forecasts["market_implied_probability"], errors="coerce"),
                }
            )
            st.subheader("Model vs market (index order, up to 100 points)")
            st.line_chart(cmp.head(100))

# --- Paper positions
where_p, p_params = build_paper_where()
pos_sql = f"""
SELECT
  p.opened_at_utc, p.run_id, p.signal_id, p.venue, p.contract_id, p.status,
  p.net_qty, p.avg_entry_price, p.mark_price, p.unrealized_pnl, p.realized_pnl, p.close_reason
FROM paper_positions p
WHERE {where_p}
ORDER BY p.opened_at_utc DESC
LIMIT 500
"""
positions = con.execute(pos_sql, p_params).df()
closed = (
    positions[positions["status"] == "closed"]
    if not positions.empty and "status" in positions.columns
    else pd.DataFrame()
)
hit_rate: float | None = None
if not closed.empty and "realized_pnl" in closed.columns:
    wins = (closed["realized_pnl"] > 0).sum()
    n = len(closed)
    hit_rate = 100.0 * float(wins) / float(n) if n else None

with tab_paper:
    st.subheader("Paper trading and PnL")
    a1, a2, a3, a4 = st.columns(4)
    if not positions.empty:
        ur = float(positions["unrealized_pnl"].sum())
        rr = float(positions["realized_pnl"].sum())
        a1.metric("Unrealized PnL (sum)", f"{ur:.4f}")
        a2.metric("Realized PnL (sum)", f"{rr:.4f}")
        n_open = int((positions["status"] == "open").sum()) if "status" in positions.columns else 0
        a3.metric("Open position rows", n_open)
        a4.metric("Closed hit rate (realized>0)", f"{hit_rate:.1f}%" if hit_rate is not None else "n/a")
        st.dataframe(positions, use_container_width=True, height=400)
        st.bar_chart(
            pd.DataFrame(
                {
                    "unrealized": [ur],
                    "realized": [rr],
                }
            )
        )
    else:
        a1.info("No paper rows in this range or filters (or none yet).")
    if positions.empty:
        st.caption("Run `pme run` to persist signals, orders, and positions.")

con.close()
