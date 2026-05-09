import logging
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
from src.theses.economic_indicators.unemployment_model import (
    build_unemployment_training_frame,
    train_validate_predict_unemployment,
    unrate_to_yes_probability,
)
from src.theses.base import ThesisModule

LOGGER = logging.getLogger(__name__)

# Kalshi series that belong to each model.
_CPI_CONTRACT_TYPES = {"cpi"}
_UNEMPLOYMENT_CONTRACT_TYPES = {"unemployment"}


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
        self.kalshi = KalshiConnector(
            api_key=settings.kalshi_api_key,
            key_id=settings.kalshi_key_id,
        )

    def ingest(self) -> dict[str, list[dict]]:
        macro = self.fred.fetch() + self.bls.fetch() + self.bea.fetch()
        macro_history = self.fred.fetch_history() + self.bls.fetch_history() + self.bea.fetch_history()
        # Try all known series ticker variants for each contract type.
        # The connector de-dupes and falls back to a generic open-market fetch
        # if all named series return empty.
        market = self.kalshi.fetch_markets([
            # CPI variants (different Kalshi naming across seasons)
            "KXCPI", "KXMCPI", "CPIM", "CPI",
            # Unemployment variants
            "KXU3", "KXECONSTATU3",
        ])
        return {"macro": macro, "macro_history": macro_history, "market": market}

    def build_features(self, raw: dict[str, list[dict]]) -> dict[str, float]:
        macro = {row["series"]: row["value"] for row in raw["macro"]}
        macro_history = raw.get("macro_history", [])

        training_df = build_training_frame_from_history(macro_history, macro)
        unemployment_training_df = build_unemployment_training_frame(
            macro_history,
            fallback_unrate=float(macro.get("UNRATE", macro.get("LNS14000000", self.settings.unemployment_threshold_pct))),
        )

        ppi = float(macro.get("PPIACO", 240.0))
        unrate = float(macro.get("UNRATE", macro.get("LNS14000000", 4.2)))
        pcepi = float(macro.get("PCEPI", 120.0))
        inflation_pressure = (ppi - 230.0) / 30.0
        pce_pressure = (pcepi - 118.0) / 10.0
        labor_slack = (unrate - 4.0) / 2.0

        return {
            "inflation_pressure": inflation_pressure,
            "pce_pressure": pce_pressure,
            "labor_slack": labor_slack,
            "market": raw["market"],
            "training_df": training_df,
            "unemployment_training_df": unemployment_training_df,
            "macro_history_count": len(macro_history),
        }

    def forecast(self, features: dict[str, float]) -> dict[str, float]:
        release_date = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        # --- CPI model ---
        cpi_reg = train_validate_predict(features["training_df"])
        cpi_mom_threshold_pct = 0.3
        cpi_model_probability = mom_percent_to_yes_probability(
            cpi_reg.prediction, threshold_pct=cpi_mom_threshold_pct, scale=12.0
        )
        cpi_val_rmse = cpi_reg.rmse
        cpi_healthy = cpi_val_rmse >= 1e-6 and abs(cpi_reg.prediction) > 1e-6
        if not cpi_healthy:
            LOGGER.warning(
                "CPI model health check failed — blocking CPI signals. "
                "val_rmse=%.2e, prediction=%.6f. "
                "Check that training frame is monthly and has non-zero CPI targets.",
                cpi_val_rmse,
                cpi_reg.prediction,
            )

        # --- Unemployment model ---
        try:
            un_reg = train_validate_predict_unemployment(features["unemployment_training_df"])
            un_healthy = un_reg.model_healthy
            if not un_healthy:
                LOGGER.warning(
                    "Unemployment model health check failed — blocking unemployment signals. "
                    "val_rmse=%.2e, prediction=%.4f pp.",
                    un_reg.rmse,
                    un_reg.prediction,
                )
        except Exception as exc:
            LOGGER.warning("Unemployment model failed: %s — using zero prediction.", exc)
            un_reg = None
            un_healthy = False

        return {
            # CPI sub-forecast
            "model_probability": cpi_model_probability,
            "predicted_cpi_mom_pct": cpi_reg.prediction,
            "cpi_mom_threshold_pct": cpi_mom_threshold_pct,
            "validation_rmse": cpi_reg.rmse,
            "validation_mae": cpi_reg.mae,
            "train_rmse": cpi_reg.train_rmse,
            "train_mae": cpi_reg.train_mae,
            "walk_forward_val_rmse": cpi_reg.walk_forward_val_rmse,
            "walk_forward_val_mae": cpi_reg.walk_forward_val_mae,
            "n_train": cpi_reg.n_train,
            "n_val": cpi_reg.n_val,
            "target_metric": cpi_reg.target_description,
            "training_start": cpi_reg.training_start,
            "training_end": cpi_reg.training_end,
            "backtest": cpi_reg.backtest,
            "model_healthy": cpi_healthy,
            # Unemployment sub-forecast
            "un_model_probability": unrate_to_yes_probability(
                un_reg.prediction if un_reg else self.settings.unemployment_threshold_pct,
                threshold=self.settings.unemployment_threshold_pct,
            ) if not un_healthy else None,
            "un_reg": un_reg,
            "un_healthy": un_healthy,
            # Shared
            "market": features["market"],
            "release_date": release_date,
            "macro_history_count": features["macro_history_count"],
        }

    def generate_signals(
        self, run_id: str, forecast: dict[str, float]
    ) -> tuple[list[SignalRecord], list[MarketSnapshotRecord]]:
        signals: list[SignalRecord] = []
        snapshots: list[MarketSnapshotRecord] = []

        cpi_healthy: bool = forecast.get("model_healthy", True)
        un_reg = forecast.get("un_reg")
        un_healthy: bool = forecast.get("un_healthy", False)

        for contract in forecast["market"]:
            bid = float(contract["best_bid"])
            ask = float(contract["best_ask"])
            mid = (bid + ask) / 2
            spread = _spread_bps(bid, ask)
            contract_type = contract.get("contract_type", "unknown")

            if contract_type in _CPI_CONTRACT_TYPES:
                model_probability = forecast["model_probability"]
                is_healthy = cpi_healthy
                decision_extras = (
                    f"pred_cpi_mom_pct={forecast.get('predicted_cpi_mom_pct', 0):.3f};"
                    f"val_rmse={forecast['validation_rmse']:.4f};"
                    f"wf_val_rmse={forecast.get('walk_forward_val_rmse', 0):.4f};"
                    f"history_rows={forecast['macro_history_count']}"
                )
                model_version = "econ_regression_v2"
                feature_version = "econ_features_v2"

            elif contract_type in _UNEMPLOYMENT_CONTRACT_TYPES:
                # Parse the threshold out of this specific contract's ticker so each
                # strike gets its own correctly-calibrated probability.
                threshold = contract.get("threshold") or self.settings.unemployment_threshold_pct
                if un_healthy and un_reg is not None:
                    # Scale the sigmoid by model quality: steeper when RMSE is low
                    # (confident), flatter when RMSE is high (uncertain).
                    # scale = min(30, 3 / rmse) → rmse=0.1 → 30, rmse=0.89 → 3.4
                    un_scale = min(30.0, 3.0 / un_reg.rmse) if un_reg.rmse >= 1e-6 else 5.0
                    model_probability = unrate_to_yes_probability(
                        un_reg.prediction, threshold=threshold, scale=un_scale
                    )
                else:
                    model_probability = 0.5  # neutral when model is unhealthy
                is_healthy = un_healthy
                decision_extras = (
                    f"pred_unrate={un_reg.prediction:.3f};" if un_reg else "pred_unrate=N/A;"
                ) + (
                    f"threshold={threshold:.1f};"
                    f"val_rmse={un_reg.rmse:.4f};" if un_reg else "val_rmse=N/A;"
                ) + (
                    f"wf_val_rmse={un_reg.walk_forward_val_rmse:.4f};" if un_reg else ""
                ) + f"history_rows={forecast['macro_history_count']}"
                model_version = "unrate_ar_v1"
                feature_version = "unrate_ar_features_v1"

            else:
                # Unknown contract type — always hold.
                model_probability = mid
                is_healthy = False
                decision_extras = f"contract_type=unknown;series={contract.get('series_ticker', '')}"
                model_version = "none"
                feature_version = "none"

            edge_bps = (model_probability - mid) * 10000

            if not is_healthy:
                decision = "hold"
                health_note = ";model_healthy=false;blocked_by_health_gate"
            elif edge_bps > self.settings.edge_threshold_bps:
                decision = "enter_long_yes"
                health_note = ""
            elif edge_bps < (-1 * self.settings.edge_threshold_bps):
                decision = "enter_long_no"
                health_note = ""
            else:
                decision = "hold"
                health_note = ""

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
                    f"contract_type={contract_type};"
                    f"model_vs_mid_edge_bps={edge_bps:.2f};"
                    + decision_extras
                    + health_note
                    + (";data_source=kalshi_stub" if contract.get("is_stub") else "")
                ),
                model_version=model_version,
                feature_set_version=feature_version,
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
        records = [
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
        un_reg = forecast.get("un_reg")
        if un_reg is not None and forecast.get("un_healthy"):
            records.append(
                ModelForecastRecord(
                    run_id=run_id,
                    thesis_module=self.name,
                    release_date_utc=forecast["release_date"],
                    model_probability=forecast.get("un_model_probability") or 0.5,
                    target_metric="UNRATE_level_ahead_1m_pp_FRED",
                    model_version="unrate_ar_v1",
                    feature_set_version="unrate_ar_features_v1",
                    training_start_utc=un_reg.training_start,
                    training_end_utc=un_reg.training_end,
                    validation_rmse=un_reg.rmse,
                    validation_mae=un_reg.mae,
                )
            )
        return records

    def paper_trade(
        self, signals: list[SignalRecord]
    ) -> tuple[list[PaperOrderRecord], list[PaperPositionRecord]]:
        return simulate_paper_trades(signals, self.settings)
