from pathlib import Path

import pytest

from src.core.config import Settings
from src.core.schemas import PaperPositionRecord
import src.pipeline.run as run_module


class _FakeKalshi:
    def __init__(self, results: dict[str, str | None] | None = None) -> None:
        self.results = results or {}
        self.requested: list[str] = []

    def fetch_market_result(self, ticker: str) -> str | None:
        self.requested.append(ticker)
        return self.results.get(ticker)


class _FakeThesis:
    def __init__(self, kalshi: _FakeKalshi) -> None:
        self.kalshi = kalshi

    def ingest(self) -> dict:
        return {}

    def build_features(self, raw: dict) -> dict:
        return raw

    def forecast(self, features: dict) -> dict:
        return features

    def build_forecast_records(self, run_id: str, forecast: dict) -> list:
        return []

    def generate_signals(self, run_id: str, forecast: dict) -> tuple[list, list]:
        return [], []

    def paper_trade(self, signals: list) -> tuple[list, list]:
        return [], []


class _FakeStorage:
    def __init__(self, stale_positions: list[PaperPositionRecord]) -> None:
        self.stale_positions = stale_positions
        self.stale_close_calls: list[tuple[float, list[str] | None]] = []
        self.close_position_batches: list[list] = []

    def upsert_run_manifest(self, manifest) -> None:
        pass

    def mark_open_positions(self, marks: list) -> int:
        return 0

    def get_stale_open_positions(self, max_stale_hours: float) -> list[PaperPositionRecord]:
        return self.stale_positions

    def close_positions(self, closes) -> int:
        batch = list(closes)
        self.close_position_batches.append(batch)
        return len(batch)

    def close_stale_positions(
        self, max_stale_hours: float, position_ids: list[str] | None = None
    ) -> int:
        self.stale_close_calls.append((max_stale_hours, position_ids))
        return len(position_ids or [])

    def get_open_positions(self) -> list:
        return []

    def add_to_position(self, add_to) -> bool:
        return True

    def insert_model_forecasts(self, forecast_records: list) -> None:
        pass

    def insert_signals(self, signals: list) -> None:
        pass

    def insert_snapshots(self, snapshots: list) -> None:
        pass

    def insert_orders(self, orders: list) -> None:
        pass

    def insert_positions(self, positions: list) -> None:
        pass

    def close(self) -> None:
        pass


def _position(
    position_id: str,
    venue: str,
    contract_id: str,
    *,
    direction: str = "yes",
    avg_entry_price: float = 0.25,
    net_qty: float = 10.0,
) -> PaperPositionRecord:
    return PaperPositionRecord(
        position_id=position_id,
        run_id="run",
        signal_id="sig",
        venue=venue,
        contract_id=contract_id,
        net_qty=net_qty,
        avg_entry_price=avg_entry_price,
        unrealized_pnl=-1.0,
        mark_price=0.15,
        status="open",
        direction=direction,
    )


def _run_with(
    tmp_path: Path,
    monkeypatch,
    *,
    stale_positions: list[PaperPositionRecord],
    kalshi_results: dict[str, str | None] | None = None,
) -> _FakeStorage:
    settings = Settings(
        data_dir=tmp_path,
        duckdb_path=tmp_path / "t.duckdb",
        paper_stale_position_close_hours=168.0,
        save_run_artifacts=False,
    )
    thesis = _FakeThesis(_FakeKalshi(kalshi_results))
    storage = _FakeStorage(stale_positions)

    monkeypatch.setattr(run_module, "get_settings", lambda: settings)
    monkeypatch.setattr(run_module, "build_registry", lambda settings: {"economic_indicators": thesis})
    monkeypatch.setattr(run_module, "Storage", lambda db_path: storage)
    monkeypatch.setattr(run_module, "_git_sha", lambda: "test-sha")

    run_module.run_pipeline("economic_indicators")
    return storage


def test_unresolved_kalshi_stale_positions_do_not_fall_back_to_last_mark(
    tmp_path: Path, monkeypatch
) -> None:
    storage = _run_with(
        tmp_path,
        monkeypatch,
        stale_positions=[
            _position("kalshi-pos", "kalshi", "KXCPI-UNRESOLVED"),
            _position("poly-pos", "polymarket", "POLY-STALE"),
        ],
        kalshi_results={"KXCPI-UNRESOLVED": None},
    )

    assert storage.close_position_batches[0] == []
    assert storage.stale_close_calls == [(168.0, ["poly-pos"])]


def test_finalized_kalshi_settlement_closes_at_binary_payout(
    tmp_path: Path, monkeypatch
) -> None:
    storage = _run_with(
        tmp_path,
        monkeypatch,
        stale_positions=[
            _position(
                "kalshi-yes",
                "KALSHI",
                "KXCPI-SETTLED",
                avg_entry_price=0.40,
                net_qty=10.0,
            )
        ],
        kalshi_results={"KXCPI-SETTLED": "no"},
    )

    closes = storage.close_position_batches[0]
    assert len(closes) == 1
    assert closes[0].position_id == "kalshi-yes"
    assert closes[0].avg_exit_price == 0.0
    assert closes[0].realized_pnl == pytest.approx((-0.40) * 10.0)
    assert closes[0].close_reason == "contract_settled"
    assert storage.stale_close_calls == []


def test_void_settlement_refunds_entry_price(tmp_path: Path, monkeypatch) -> None:
    storage = _run_with(
        tmp_path,
        monkeypatch,
        stale_positions=[
            _position(
                "kalshi-void",
                "kalshi",
                "KXCPI-VOID",
                avg_entry_price=0.72,
                net_qty=5.0,
            )
        ],
        kalshi_results={"KXCPI-VOID": "void"},
    )

    closes = storage.close_position_batches[0]
    assert len(closes) == 1
    assert closes[0].avg_exit_price == 0.72
    assert closes[0].realized_pnl == 0.0
    assert storage.stale_close_calls == []
