from datetime import datetime, timezone

from src.connectors.bea import BeaConnector
from src.connectors.bls import BlsConnector
from src.connectors.fred import FredConnector
from src.connectors.kalshi import KalshiConnector
from src.core.config import Settings
from src.core.schemas import (
    MarketSnapshotRecord,
    ModelForecastRecord,
    PaperOrderRecord,
    PaperPositionRecord,
    SignalRecord,
)
from src.pipeline.paper_trading import simulate_paper_trades
from src.theses.economic_indicators.model import (
    build_training_frame_from_history,
    mom_percent_to_yes_probability,
    train_validate_predict,
)
from src.theses.base import ThesisModule


def _spread_bps(bid: float, ask: float) -> float:
    mid = (bid + ask) / 2
    if mid == 0:
        return 0.0
    return ((ask - bid) / mid) * 10000


class EconomicIndicatorsThesis(ThesisModule):
    name = "economic_indicators"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.fred = FredConnector(api_key=settings.fred_api_key)
        self.bls = BlsConnector(api_key=settings.bls_api_key)
        self.bea = BeaConnector(api_key=settings.bea_api_key)
        self.kalshi = KalshiConnector()

    def ingest(self) -> dict[str, list[dict]]:
        macro = self.fred.fetch() + self.bls.fetch() + self.bea.fetch()
        macro_history = self.fred.fetch_history() + self.bls.fetch_history() + self.bea.fetch_history()
        return {"macro": macro, "macro_history": macro_history, "market": self.kalshi.fetch()}

    def build_features(self, raw: dict[str, list[dict]]) -> dict[str, float]:
        macro = {row["series"]: row["value"] for row in raw["macro"]}
        training_df = build_training_frame_from_history(raw.get("macro_history", []), macro)
        ppi = float(macro.get("PPIACO", 240.0))
        unrate = float(macro.get("UNRATE", macro.get("LNS14000000", 4.2)))
        pcepi = float(macro.get("PCEPI", 120.0))
        # Lightweight v1 feature transform; replace with proper historical modeling next.
        inflation_pressure = (ppi - 230.0) / 30.0
        pce_pressure = (pcepi - 118.0) / 10.0
        labor_slack = (unrate - 4.0) / 2.0
        return {
            "inflation_pressure": inflation_pressure,
            "pce_pressure": pce_pressure,
            "labor_slack": labor_slack,
            "market": raw["market"],
            "training_df": training_df,
            "macro_history_count": len(raw.get("macro_history", [])),
        }

    def forecast(self, features: dict[str, float]) -> dict[str, float]:
        regression = train_validate_predict(features["training_df"])
        # Predicted value is next-month CPI m/m in percent points; map to P(YES) vs a headline threshold.
        cpi_mom_threshold_pct = 0.3
        model_probability = mom_percent_to_yes_probability(
            regression.prediction, threshold_pct=cpi_mom_threshold_pct, scale=12.0
        )
        release_date = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return {
            "model_probability": model_probability,
            "predicted_cpi_mom_pct": regression.prediction,
            "cpi_mom_threshold_pct": cpi_mom_threshold_pct,
            "market": features["market"],
            "validation_rmse": regression.rmse,
            "validation_mae": regression.mae,
            "train_rmse": regression.train_rmse,
            "train_mae": regression.train_mae,
            "walk_forward_val_rmse": regression.walk_forward_val_rmse,
            "walk_forward_val_mae": regression.walk_forward_val_mae,
            "n_train": regression.n_train,
            "n_val": regression.n_val,
            "target_metric": regression.target_description,
            "training_start": regression.training_start,
            "training_end": regression.training_end,
            "backtest": regression.backtest,
            "release_date": release_date,
            "macro_history_count": features["macro_history_count"],
        }

    def generate_signals(
        self, run_id: str, forecast: dict[str, float]
    ) -> tuple[list[SignalRecord], list[MarketSnapshotRecord]]:
        signals: list[SignalRecord] = []
        snapshots: list[MarketSnapshotRecord] = []
        model_probability = forecast["model_probability"]

        for contract in forecast["market"]:
            bid = float(contract["best_bid"])
            ask = float(contract["best_ask"])
            mid = (bid + ask) / 2
            spread = _spread_bps(bid, ask)
            edge_bps = (model_probability - mid) * 10000
            decision = "hold"
            if edge_bps > self.settings.edge_threshold_bps:
                decision = "enter_long_yes"
            elif edge_bps < (-1 * self.settings.edge_threshold_bps):
                decision = "enter_long_no"

            signal = SignalRecord(
                run_id=run_id,
                thesis_module=self.name,
                venue=contract["venue"],
                contract_id=contract["contract_id"],
                contract_label=contract["label"],
                model_probability=model_probability,
                market_implied_probability=mid,
                edge_bps=edge_bps,
                bid_price=bid,
                ask_price=ask,
                spread_bps=spread,
                vig_adjusted_threshold_bps=float(self.settings.edge_threshold_bps),
                decision=decision,
                decision_reason=(
                    f"model_vs_mid_edge_bps={edge_bps:.2f};"
                    f"pred_cpi_mom_pct={forecast.get('predicted_cpi_mom_pct', 0):.3f};"
                    f"val_rmse={forecast['validation_rmse']:.4f};"
                    f"wf_val_rmse={forecast.get('walk_forward_val_rmse', 0):.4f};"
                    f"history_rows={forecast['macro_history_count']}"
                ),
                model_version="econ_regression_v2",
                feature_set_version="econ_features_v2",
                assumption_version=self.settings.paper_assumption_version,
            )
            signals.append(signal)

            snapshots.append(
                MarketSnapshotRecord(
                    venue=contract["venue"],
                    contract_id=contract["contract_id"],
                    best_bid=bid,
                    best_ask=ask,
                    last_trade=float(contract["last_trade"]),
                    mid_price=mid,
                    spread_bps=spread,
                )
            )
        return signals, snapshots

    def build_forecast_records(self, run_id: str, forecast: dict[str, float]) -> list[ModelForecastRecord]:
        return [
            ModelForecastRecord(
                run_id=run_id,
                thesis_module=self.name,
                release_date_utc=forecast["release_date"],
                model_probability=forecast["model_probability"],
                target_metric=str(forecast.get("target_metric", "CPI_mom_ahead_1m_pct_FRED_CPIAUCSL"))[:200],
                model_version="econ_regression_v2",
                feature_set_version="econ_features_v2",
                training_start_utc=forecast["training_start"],
                training_end_utc=forecast["training_end"],
                validation_rmse=forecast["validation_rmse"],
                validation_mae=forecast["validation_mae"],
            )
        ]

    def paper_trade(
        self, signals: list[SignalRecord]
    ) -> tuple[list[PaperOrderRecord], list[PaperPositionRecord]]:
        return simulate_paper_trades(signals, self.settings)
