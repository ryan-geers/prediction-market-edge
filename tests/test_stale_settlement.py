from datetime import timedelta
from pathlib import Path

from src.core.schemas import PaperPositionRecord, utc_now
from src.core.storage import Storage
from src.pipeline.run import _resolve_stale_positions


class _FakeKalshi:
    def __init__(self, results: dict[str, str | None]) -> None:
        self.results = results

    def fetch_market_result(self, ticker: str) -> str | None:
        return self.results.get(ticker)


def _stale_position(
    *,
    position_id: str,
    venue: str = "kalshi",
    contract_id: str = "KALSHI-TEST",
    avg_entry_price: float = 0.40,
    net_qty: float = 10.0,
    direction: str = "yes",
) -> PaperPositionRecord:
    return PaperPositionRecord(
        position_id=position_id,
        run_id="run-test",
        signal_id="sig-test",
        venue=venue,
        contract_id=contract_id,
        net_qty=net_qty,
        avg_entry_price=avg_entry_price,
        unrealized_pnl=1.23,
        mark_price=0.52,
        last_mark_time_utc=utc_now() - timedelta(hours=200),
        status="open",
        direction=direction,
    )


def test_unresolved_kalshi_stale_position_stays_open(tmp_path: Path) -> None:
    st = Storage(tmp_path / "t.duckdb")
    kalshi_pos = _stale_position(position_id="kalshi-open", contract_id="KALSHI-OPEN")
    poly_pos = _stale_position(
        position_id="poly-stale",
        venue="polymarket",
        contract_id="POLY-STALE",
    )
    st.insert_positions([kalshi_pos, poly_pos])

    stale_positions = st.get_stale_open_positions(max_stale_hours=168)
    settled, stale_closed = _resolve_stale_positions(
        st,
        stale_positions,
        _FakeKalshi({"KALSHI-OPEN": None}),
        max_stale_hours=168,
    )

    rows = {
        row[0]: (row[1], row[2])
        for row in st.con.execute(
            "SELECT position_id, status, close_reason FROM paper_positions ORDER BY position_id"
        ).fetchall()
    }
    st.close()

    assert settled == 0
    assert stale_closed == 1
    assert rows["kalshi-open"] == ("open", None)
    assert rows["poly-stale"] == ("closed", "stale_no_market")


def test_final_kalshi_settlement_closes_at_result_or_refund(tmp_path: Path) -> None:
    st = Storage(tmp_path / "t.duckdb")
    yes_winner = _stale_position(
        position_id="yes-winner",
        contract_id="KALSHI-YES",
        avg_entry_price=0.40,
        net_qty=10.0,
        direction="yes",
    )
    voided = _stale_position(
        position_id="voided",
        contract_id="KALSHI-VOID",
        avg_entry_price=0.73,
        net_qty=20.0,
        direction="no",
    )
    st.insert_positions([yes_winner, voided])

    stale_positions = st.get_stale_open_positions(max_stale_hours=168)
    settled, stale_closed = _resolve_stale_positions(
        st,
        stale_positions,
        _FakeKalshi({"KALSHI-YES": "yes", "KALSHI-VOID": "void"}),
        max_stale_hours=168,
    )

    rows = {
        row[0]: (row[1], row[2], row[3], row[4])
        for row in st.con.execute(
            """
            SELECT position_id, status, avg_exit_price, realized_pnl, close_reason
            FROM paper_positions ORDER BY position_id
            """
        ).fetchall()
    }
    st.close()

    assert settled == 2
    assert stale_closed == 0
    assert rows["yes-winner"] == ("closed", 1.0, 6.0, "contract_settled")
    assert rows["voided"] == ("closed", 0.73, 0.0, "contract_settled")
