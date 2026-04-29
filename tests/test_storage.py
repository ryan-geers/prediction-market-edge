"""
Unit tests for Storage.mark_open_positions (Phase 1),
Storage.close_positions / Storage.get_open_positions (Phase 2), and
Storage.get_open_position / Storage.add_to_position (Phase 3).
"""
from pathlib import Path

import duckdb

from src.core.schemas import AddToPosition, PaperPositionRecord, PositionClose, PositionMark, utc_now
from src.core.storage import Storage


def _open_position(
    position_id: str = "pos-1",
    contract_id: str = "CPI-TEST",
    venue: str = "KALSHI",
    avg_entry_price: float = 0.50,
    net_qty: float = 50.0,
) -> PaperPositionRecord:
    return PaperPositionRecord(
        position_id=position_id,
        run_id="r-test",
        signal_id="sig-test",
        venue=venue,
        contract_id=contract_id,
        net_qty=net_qty,
        avg_entry_price=avg_entry_price,
        unrealized_pnl=0.0,
        mark_price=avg_entry_price,
        status="open",
    )


def test_mark_updates_mark_price_and_unrealized_pnl(tmp_path: Path) -> None:
    """mark_open_positions sets mark_price and recalculates unrealized_pnl."""
    st = Storage(tmp_path / "t.duckdb")
    pos = _open_position(avg_entry_price=0.50, net_qty=50.0)
    st.insert_positions([pos])

    mark = PositionMark(contract_id="CPI-TEST", venue="KALSHI", mark_price=0.60)
    updated = st.mark_open_positions([mark])
    st.close()

    assert updated == 1
    con = duckdb.connect(str(tmp_path / "t.duckdb"))
    row = con.execute(
        "SELECT mark_price, unrealized_pnl FROM paper_positions WHERE position_id = ?",
        [pos.position_id],
    ).fetchone()
    con.close()

    assert row is not None
    assert abs(row[0] - 0.60) < 1e-9
    # unrealized_pnl = (mark_price - avg_entry_price) * net_qty = (0.60 - 0.50) * 50 = 5.0
    assert abs(row[1] - 5.0) < 1e-9


def test_mark_does_not_update_closed_positions(tmp_path: Path) -> None:
    """Closed positions must not be re-marked."""
    st = Storage(tmp_path / "t.duckdb")
    pos = _open_position(avg_entry_price=0.50, net_qty=50.0)
    closed = pos.model_copy(
        update={"position_id": "pos-closed", "status": "closed", "mark_price": 0.50}
    )
    st.insert_positions([pos, closed])

    mark = PositionMark(contract_id="CPI-TEST", venue="KALSHI", mark_price=0.70)
    updated = st.mark_open_positions([mark])
    st.close()

    assert updated == 1  # only the open row

    con = duckdb.connect(str(tmp_path / "t.duckdb"))
    closed_row = con.execute(
        "SELECT mark_price FROM paper_positions WHERE position_id = 'pos-closed'"
    ).fetchone()
    con.close()
    # Closed row must be unchanged
    assert closed_row is not None
    assert abs(closed_row[0] - 0.50) < 1e-9


def test_mark_multiple_contracts(tmp_path: Path) -> None:
    """Marks for different contracts update only their own rows."""
    st = Storage(tmp_path / "t.duckdb")
    pos_a = _open_position(
        position_id="pos-a", contract_id="CPI-A", avg_entry_price=0.40, net_qty=25.0
    )
    pos_b = _open_position(
        position_id="pos-b", contract_id="CPI-B", avg_entry_price=0.60, net_qty=10.0
    )
    st.insert_positions([pos_a, pos_b])

    marks = [
        PositionMark(contract_id="CPI-A", venue="KALSHI", mark_price=0.45),
        PositionMark(contract_id="CPI-B", venue="KALSHI", mark_price=0.55),
    ]
    updated = st.mark_open_positions(marks)
    st.close()

    assert updated == 2

    con = duckdb.connect(str(tmp_path / "t.duckdb"))
    rows = {
        r[0]: (r[1], r[2])
        for r in con.execute(
            "SELECT position_id, mark_price, unrealized_pnl FROM paper_positions"
        ).fetchall()
    }
    con.close()

    # CPI-A: (0.45 - 0.40) * 25 = 1.25
    assert abs(rows["pos-a"][0] - 0.45) < 1e-9
    assert abs(rows["pos-a"][1] - 1.25) < 1e-9
    # CPI-B: (0.55 - 0.60) * 10 = -0.50
    assert abs(rows["pos-b"][0] - 0.55) < 1e-9
    assert abs(rows["pos-b"][1] - (-0.50)) < 1e-9


def test_mark_no_matching_open_positions_returns_zero(tmp_path: Path) -> None:
    """mark_open_positions returns 0 when no open rows match the contract."""
    st = Storage(tmp_path / "t.duckdb")
    mark = PositionMark(contract_id="NO-SUCH-CONTRACT", venue="KALSHI", mark_price=0.50)
    updated = st.mark_open_positions([mark])
    st.close()
    assert updated == 0


# ── Phase 2: close_positions / get_open_positions ─────────────────────────────

def test_close_positions_updates_status_and_pnl(tmp_path: Path) -> None:
    """close_positions flips status to 'closed' and writes exit details."""
    st = Storage(tmp_path / "t.duckdb")
    pos = _open_position(avg_entry_price=0.50, net_qty=50.0)
    st.insert_positions([pos])

    close = PositionClose(
        position_id=pos.position_id,
        avg_exit_price=0.65,
        realized_pnl=7.50,
        close_reason="signal_flip",
    )
    updated = st.close_positions([close])
    st.close()

    assert updated == 1

    con = duckdb.connect(str(tmp_path / "t.duckdb"))
    row = con.execute(
        """
        SELECT status, avg_exit_price, realized_pnl, unrealized_pnl, close_reason
        FROM paper_positions WHERE position_id = ?
        """,
        [pos.position_id],
    ).fetchone()
    con.close()

    assert row is not None
    assert row[0] == "closed"
    assert abs(row[1] - 0.65) < 1e-9
    assert abs(row[2] - 7.50) < 1e-9
    assert row[3] == 0.0
    assert row[4] == "signal_flip"


def test_close_positions_does_not_affect_other_rows(tmp_path: Path) -> None:
    """Closing one position leaves other open positions untouched."""
    st = Storage(tmp_path / "t.duckdb")
    pos_a = _open_position(position_id="pos-a", contract_id="CPI-A")
    pos_b = _open_position(position_id="pos-b", contract_id="CPI-B")
    st.insert_positions([pos_a, pos_b])

    close = PositionClose(
        position_id="pos-a",
        avg_exit_price=0.60,
        realized_pnl=5.0,
        close_reason="stop_loss",
    )
    st.close_positions([close])
    st.close()

    con = duckdb.connect(str(tmp_path / "t.duckdb"))
    status_b = con.execute(
        "SELECT status FROM paper_positions WHERE position_id = 'pos-b'"
    ).fetchone()
    con.close()
    assert status_b is not None and status_b[0] == "open"


def test_get_open_positions_returns_only_open(tmp_path: Path) -> None:
    """get_open_positions excludes closed rows."""
    st = Storage(tmp_path / "t.duckdb")
    open_pos = _open_position(position_id="pos-open")
    closed_pos = _open_position(position_id="pos-closed")
    st.insert_positions([open_pos, closed_pos])

    # Close one
    st.close_positions([
        PositionClose(
            position_id="pos-closed",
            avg_exit_price=0.55,
            realized_pnl=2.5,
            close_reason="manual",
        )
    ])

    result = st.get_open_positions()
    st.close()

    assert len(result) == 1
    assert result[0].position_id == "pos-open"


def test_get_open_positions_includes_direction(tmp_path: Path) -> None:
    """get_open_positions round-trips the direction field."""
    st = Storage(tmp_path / "t.duckdb")
    pos = _open_position(position_id="pos-yes")
    pos = pos.model_copy(update={"direction": "yes"})
    st.insert_positions([pos])

    result = st.get_open_positions()
    st.close()

    assert result[0].direction == "yes"


def test_mark_updates_last_mark_time(tmp_path: Path) -> None:
    """last_mark_time_utc is updated to the mark's timestamp."""
    st = Storage(tmp_path / "t.duckdb")
    pos = _open_position()
    st.insert_positions([pos])

    now = utc_now()
    mark = PositionMark(
        contract_id="CPI-TEST", venue="KALSHI", mark_price=0.55, last_mark_time_utc=now
    )
    st.mark_open_positions([mark])
    st.close()

    con = duckdb.connect(str(tmp_path / "t.duckdb"))
    ts = con.execute(
        "SELECT last_mark_time_utc FROM paper_positions WHERE position_id = ?",
        [pos.position_id],
    ).fetchone()
    con.close()
    assert ts is not None and ts[0] is not None


# ── Phase 3: get_open_position / add_to_position ───────────────────────────────

def test_get_open_position_returns_matching_row(tmp_path: Path) -> None:
    """get_open_position finds the right row by (contract_id, venue, direction)."""
    st = Storage(tmp_path / "t.duckdb")
    pos = _open_position()
    pos = pos.model_copy(update={"direction": "yes"})
    st.insert_positions([pos])

    result = st.get_open_position("CPI-TEST", "KALSHI", "yes")
    st.close()

    assert result is not None
    assert result.position_id == pos.position_id
    assert result.direction == "yes"


def test_get_open_position_returns_none_when_missing(tmp_path: Path) -> None:
    """get_open_position returns None when no open row matches."""
    st = Storage(tmp_path / "t.duckdb")
    result = st.get_open_position("NO-CONTRACT", "KALSHI", "yes")
    st.close()
    assert result is None


def test_get_open_position_ignores_closed_rows(tmp_path: Path) -> None:
    """A closed row with the same key is not returned."""
    st = Storage(tmp_path / "t.duckdb")
    pos = _open_position()
    pos = pos.model_copy(update={"direction": "yes"})
    st.insert_positions([pos])
    st.close_positions([
        PositionClose(
            position_id=pos.position_id,
            avg_exit_price=0.55,
            realized_pnl=2.5,
            close_reason="manual",
        )
    ])

    result = st.get_open_position("CPI-TEST", "KALSHI", "yes")
    st.close()
    assert result is None


def test_add_to_position_vwap_and_qty(tmp_path: Path) -> None:
    """add_to_position updates net_qty, avg_entry_price and recomputes unrealized_pnl."""
    st = Storage(tmp_path / "t.duckdb")
    # Insert open position already marked at 0.55
    pos = _open_position(avg_entry_price=0.50, net_qty=50.0)
    pos = pos.model_copy(update={"direction": "yes", "mark_price": 0.55})
    st.insert_positions([pos])

    # VWAP: (0.50*50 + 0.60*40) / 90 = 49/90 ≈ 0.5444...
    new_qty = 90.0
    new_avg = (0.50 * 50.0 + 0.60 * 40.0) / new_qty
    op = AddToPosition(position_id=pos.position_id, new_net_qty=new_qty, new_avg_entry_price=new_avg)
    success = st.add_to_position(op)
    st.close()

    assert success is True

    con = duckdb.connect(str(tmp_path / "t.duckdb"))
    row = con.execute(
        "SELECT net_qty, avg_entry_price, unrealized_pnl FROM paper_positions WHERE position_id = ?",
        [pos.position_id],
    ).fetchone()
    con.close()

    assert row is not None
    assert abs(row[0] - new_qty) < 1e-9
    assert abs(row[1] - new_avg) < 1e-9
    # unrealized_pnl = (mark_price - new_avg) * new_qty = (0.55 - new_avg) * 90
    expected_unrealized = (0.55 - new_avg) * new_qty
    assert abs(row[2] - expected_unrealized) < 1e-9


def test_add_to_position_returns_false_for_unknown_id(tmp_path: Path) -> None:
    """add_to_position returns False when position_id does not exist."""
    st = Storage(tmp_path / "t.duckdb")
    op = AddToPosition(position_id="ghost-id", new_net_qty=10.0, new_avg_entry_price=0.50)
    result = st.add_to_position(op)
    st.close()
    assert result is False
