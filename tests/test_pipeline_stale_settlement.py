from pathlib import Path

from src.core.config import Settings
from src.core.schemas import PaperPositionRecord
import src.pipeline.run as run_module


class _FakeKalshi:
    def __init__(self) -> None:
        self.requested: list[str] = []

    def fetch_market_result(self, ticker: str) -> str | None:
        self.requested.append(ticker)
        return None


class _FakeThesis:
    def __init__(self) -> None:
        self.kalshi = _FakeKalshi()

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


def _position(position_id: str, venue: str, contract_id: str) -> PaperPositionRecord:
    return PaperPositionRecord(
        position_id=position_id,
        run_id="run",
        signal_id="sig",
        venue=venue,
        contract_id=contract_id,
        net_qty=10.0,
        avg_entry_price=0.25,
        unrealized_pnl=-1.0,
        mark_price=0.15,
        status="open",
        direction="yes",
    )


def test_unresolved_kalshi_stale_positions_do_not_fall_back_to_last_mark(
    tmp_path: Path, monkeypatch
) -> None:
    settings = Settings(
        data_dir=tmp_path,
        duckdb_path=tmp_path / "t.duckdb",
        paper_stale_position_close_hours=168.0,
        save_run_artifacts=False,
    )
    thesis = _FakeThesis()
    storage = _FakeStorage(
        [
            _position("kalshi-pos", "kalshi", "KXCPI-UNRESOLVED"),
            _position("poly-pos", "polymarket", "POLY-STALE"),
        ]
    )

    monkeypatch.setattr(run_module, "get_settings", lambda: settings)
    monkeypatch.setattr(run_module, "build_registry", lambda settings: {"economic_indicators": thesis})
    monkeypatch.setattr(run_module, "Storage", lambda db_path: storage)
    monkeypatch.setattr(run_module, "_git_sha", lambda: "test-sha")

    run_id, report_path = run_module.run_pipeline("economic_indicators")

    assert run_id
    assert report_path is None
    assert thesis.kalshi.requested == ["KXCPI-UNRESOLVED"]
    assert storage.close_position_batches[0] == []
    assert storage.stale_close_calls == [(168.0, ["poly-pos"])]
