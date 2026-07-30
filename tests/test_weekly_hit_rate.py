"""Weekly digest must not count bookkeeping closes in hit-rate / lifetime averages."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import duckdb

from src.core.schemas import PaperPositionRecord
from src.core.storage import Storage
from src.pipeline.reporting import _weekly_payload


def _closed(
    *,
    realized_pnl: float,
    close_reason: str,
    contract_id: str,
    closed_at: datetime,
) -> PaperPositionRecord:
    return PaperPositionRecord(
        position_id=str(uuid4()),
        run_id="r-hit",
        signal_id=str(uuid4()),
        venue="kalshi",
        contract_id=contract_id,
        opened_at_utc=closed_at - timedelta(days=2),
        closed_at_utc=closed_at,
        net_qty=0.0 if close_reason in {"dedup_consolidated", "zero_qty_cleanup"} else 10.0,
        avg_entry_price=0.40,
        avg_exit_price=0.40,
        realized_pnl=realized_pnl,
        unrealized_pnl=0.0,
        mark_price=0.40,
        last_mark_time_utc=closed_at - timedelta(hours=1),
        status="closed",
        close_reason=close_reason,
        direction="yes",
    )


def test_weekly_hit_rate_excludes_dedup_and_zero_qty_closes(tmp_path: Path) -> None:
    """
    Concrete trigger: one real win (+10), one real loss (-5), eight dedup_consolidated
    zeros from consolidate-positions. Pre-fix hit rate was 1/10 = 10% and lifetime
    average was 0.5; tradable closes alone are 50% / 2.5.
    """
    db = tmp_path / "hit.duckdb"
    st = Storage(db)
    now = datetime.now(timezone.utc)
    st.insert_positions(
        [
            _closed(realized_pnl=10.0, close_reason="signal_flip", contract_id="KXCPI-A", closed_at=now),
            _closed(realized_pnl=-5.0, close_reason="stop_loss", contract_id="KXCPI-B", closed_at=now),
            *[
                _closed(
                    realized_pnl=0.0,
                    close_reason="dedup_consolidated",
                    contract_id=f"KXCPI-D{i}",
                    closed_at=now,
                )
                for i in range(8)
            ],
            _closed(
                realized_pnl=0.0,
                close_reason="zero_qty_cleanup",
                contract_id="KXCPI-Z",
                closed_at=now,
            ),
        ]
    )
    st.close()

    con = duckdb.connect(str(db))
    try:
        w = _weekly_payload(con, now - timedelta(days=7))
    finally:
        con.close()

    assert w["closed_n"] == 2
    assert w["closed_n_positive"] == 1
    assert w["closed_n_negative"] == 1
    assert w["closed_n_zero"] == 0
    assert w["hit_rate"] == 50.0
    assert w["lifetime_closed_n"] == 2
    assert abs(float(w["lifetime_avg_realized"]) - 2.5) < 1e-9
    assert abs(float(w["weekly_closed_realized_sum"]) - 5.0) < 1e-9
    # Recent exits list should also omit bookkeeping rows.
    assert len(w["exits_window"]) == 2
