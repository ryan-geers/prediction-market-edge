from src.core.schemas import MarketSnapshotRecord, PaperOrderRecord, PaperPositionRecord, SignalRecord
from src.theses.base import ThesisModule


class StateLocalElectionsThesis(ThesisModule):
    name = "state_local_elections"

    def ingest(self) -> dict:
        return {}

    def build_features(self, raw: dict) -> dict:
        return {}

    def forecast(self, features: dict) -> dict:
        return {}

    def generate_signals(self, run_id: str, forecast: dict) -> tuple[list[SignalRecord], list[MarketSnapshotRecord]]:
        return [], []

    def paper_trade(self, signals: list[SignalRecord]) -> tuple[list[PaperOrderRecord], list[PaperPositionRecord]]:
        return [], []
