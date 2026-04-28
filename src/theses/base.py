from abc import ABC, abstractmethod
from typing import Any

from src.core.schemas import (
    MarketSnapshotRecord,
    ModelForecastRecord,
    PaperOrderRecord,
    PaperPositionRecord,
    SignalRecord,
)


class ThesisModule(ABC):
    name: str

    @abstractmethod
    def ingest(self) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def build_features(self, raw: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def forecast(self, features: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def generate_signals(
        self, run_id: str, forecast: dict[str, Any]
    ) -> tuple[list[SignalRecord], list[MarketSnapshotRecord]]:
        raise NotImplementedError

    @abstractmethod
    def paper_trade(
        self, signals: list[SignalRecord]
    ) -> tuple[list[PaperOrderRecord], list[PaperPositionRecord]]:
        raise NotImplementedError

    def build_forecast_records(self, run_id: str, forecast: dict[str, Any]) -> list[ModelForecastRecord]:
        return []
