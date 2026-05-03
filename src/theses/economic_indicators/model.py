import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

LOGGER = logging.getLogger(__name__)

# FRED: CPI for All Urban Consumers, seasonally adjusted (index level; mom % from levels).
CPI_SERIES_FRED = "CPIAUCSL"


@dataclass
class RegressionResult:
    """OLS on [ppi, pcepi, unrate] -> one-month-ahead CPI m/m % (annualized not used; plain mom)."""

    prediction: float
    rmse: float
    mae: float
    training_start: datetime
    training_end: datetime
    train_rmse: float
    train_mae: float
    n_train: int
    n_val: int
    target_column: str
    target_description: str
    # Simple pseudo walk-forward on the chronological validation tail (re-fit OLS on prefix).
    walk_forward_val_rmse: float
    walk_forward_val_mae: float
    backtest: dict = field(default_factory=dict)


def build_training_frame(macro: dict[str, float], periods: int = 36) -> pd.DataFrame:
    """Synthetic monthly frame when live history is insufficient. Target: proxy next-month m/m %."""
    now = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    base_ppi = float(macro.get("PPIACO", 245.0))
    base_unrate = float(macro.get("UNRATE", macro.get("LNS14000000", 4.2)))
    base_pce = float(macro.get("PCEPI", 123.0))

    rows: list[dict] = []
    for i in range(periods, 0, -1):
        dt = now - timedelta(days=30 * i)
        trend = (periods - i) / periods
        ppi = base_ppi * (0.96 + 0.08 * trend)
        pce = base_pce * (0.97 + 0.06 * trend)
        unrate = max(3.0, base_unrate + (0.3 - (0.6 * trend)))
        # Proxy next-month m/m in % (typical 0.1–0.6 range for headline CPI)
        blend = (0.16 * (ppi / 100)) + (0.27 * (pce / 100)) - (0.04 * unrate) + 0.18
        cpi_mom_next = (blend * 0.9 + 0.02) * 100.0 / 100.0  # keep in ~0.15–0.55
        rows.append(
            {
                "release_date": dt,
                "ppi": ppi,
                "pcepi": pce,
                "unrate": unrate,
                "cpi_mom_next": max(0.05, min(0.8, cpi_mom_next)),
            }
        )
    return pd.DataFrame(rows)


def build_training_frame_from_history(
    history_rows: list[dict], fallback_macro: dict[str, float | int]
) -> pd.DataFrame:
    if not history_rows:
        return build_training_frame(fallback_macro)  # type: ignore[arg-type]

    frame = pd.DataFrame(history_rows)
    if frame.empty or "series" not in frame or "date" not in frame:
        return build_training_frame(fallback_macro)  # type: ignore[arg-type]

    frame["date"] = pd.to_datetime(frame["date"], errors="coerce", utc=True)
    frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
    frame = frame.dropna(subset=["date", "value"])
    if frame.empty:
        return build_training_frame(fallback_macro)  # type: ignore[arg-type]

    pivot = frame.pivot_table(index="date", columns="series", values="value", aggfunc="last").sort_index()
    if "UNRATE" not in pivot.columns and "LNS14000000" in pivot.columns:
        pivot["UNRATE"] = pivot["LNS14000000"]

    feature_cols = ["PPIACO", "PCEPI", "UNRATE"]
    for col in feature_cols:
        if col not in pivot.columns:
            pivot[col] = np.nan
    for col in feature_cols:
        pivot[col] = pivot[col].ffill()
    if CPI_SERIES_FRED in pivot.columns:
        pivot[CPI_SERIES_FRED] = pivot[CPI_SERIES_FRED].ffill()

    pivot = pivot.dropna(subset=feature_cols, how="any")
    if len(pivot) < 3:
        return build_training_frame(fallback_macro)  # type: ignore[arg-type]

    if CPI_SERIES_FRED in pivot.columns and pivot[CPI_SERIES_FRED].notna().sum() >= 3:
        cpi = pivot[CPI_SERIES_FRED]
        # One-month-ahead m/m % change realized at t+1 (label for row t uses info through t)
        cpi_mom_next = (cpi.shift(-1) / cpi - 1.0) * 100.0
        train = pivot[feature_cols].copy()
        train["cpi_mom_next"] = cpi_mom_next
        train = train.dropna()
        train = train.rename(
            columns={"PPIACO": "ppi", "PCEPI": "pcepi", "UNRATE": "unrate"}
        )
        train = train.reset_index().rename(columns={"date": "release_date"})
        if len(train) < 8:
            return build_training_frame(fallback_macro)  # type: ignore[arg-type]
        return train[["release_date", "ppi", "pcepi", "unrate", "cpi_mom_next"]].sort_values("release_date")

    # No usable CPI level series: fall back to proxy (shifted composite), still named cpi_mom_next
    train = pivot[feature_cols].copy()
    proxy = (0.16 * (train["PPIACO"] / 100) + 0.27 * (train["PCEPI"] / 100) - 0.04 * train["UNRATE"] + 0.22)
    train["cpi_mom_next"] = (proxy * 0.45 + 0.08).shift(-1)
    train = train.dropna()
    train = train.rename(columns={"PPIACO": "ppi", "PCEPI": "pcepi", "UNRATE": "unrate"})
    train = train.reset_index().rename(columns={"date": "release_date"})
    if len(train) < 8:
        return build_training_frame(fallback_macro)  # type: ignore[arg-type]
    return train[["release_date", "ppi", "pcepi", "unrate", "cpi_mom_next"]].sort_values("release_date")


def train_validate_predict(
    df: pd.DataFrame,
    val_fraction: float = 0.2,
    min_train_rows: int = 8,
) -> RegressionResult:
    """
    Time-ordered OLS; last `val_fraction` rows are validation. Prediction uses latest feature row.
    Adds train-set metrics and a small walk-forward on validation rows (re-fit on expanding history).
    """
    d = df.sort_values("release_date").reset_index(drop=True)
    y = d["cpi_mom_next"].to_numpy(dtype=float)
    X = d[["ppi", "pcepi", "unrate"]].to_numpy(dtype=float)
    n = len(d)
    if n < min_train_rows + 2:
        raise ValueError("Insufficient rows for time-series validation")

    split_idx = max(min_train_rows, int(n * (1.0 - val_fraction)))
    split_idx = min(split_idx, n - 1)  # need >=1 val
    if split_idx < 2:
        split_idx = n // 2

    X_train, X_val = X[:split_idx], X[split_idx:]
    y_train, y_val = y[:split_idx], y[split_idx:]

    train_design = np.c_[np.ones(X_train.shape[0]), X_train]
    coef, _, _, _ = np.linalg.lstsq(train_design, y_train, rcond=None)

    y_hat_train = train_design @ coef
    train_rmse = float(np.sqrt(np.mean((y_train - y_hat_train) ** 2)))
    train_mae = float(np.mean(np.abs(y_train - y_hat_train)))

    if len(y_val) == 0:
        raise ValueError(
            f"Validation split is empty (n={n}, split_idx={split_idx}). "
            "Cannot compute model quality metrics — refusing to generate signals on an unvalidated model."
        )

    val_design = np.c_[np.ones(X_val.shape[0]), X_val]
    y_hat_val = val_design @ coef
    rmse = float(np.sqrt(np.mean((y_val - y_hat_val) ** 2)))
    mae = float(np.mean(np.abs(y_val - y_hat_val)))

    if rmse < 1e-6:
        LOGGER.warning(
            "Suspiciously low validation RMSE (%.2e) — likely caused by a proxy target "
            "that is a near-linear function of the training features (target leakage). "
            "n_train=%d, n_val=%d",
            rmse,
            split_idx,
            len(y_val),
        )

    # Walk-forward on validation: for each k, train on 0:split_idx+k, predict one step at k
    wf_errors: list[float] = []
    for k in range(len(y_val)):
        end = split_idx + k
        Xw = X[:end]
        yw = y[:end]
        if Xw.shape[0] < 3:
            continue
        des = np.c_[np.ones(Xw.shape[0]), Xw]
        wcoef, _, _, _ = np.linalg.lstsq(des, yw, rcond=None)
        x_next = np.r_[1.0, X[end]]
        pred = float(x_next @ wcoef)
        wf_errors.append(y[end] - pred)
    if wf_errors:
        wf_res = np.array(wf_errors, dtype=float)
        wf_rmse = float(np.sqrt(np.mean(wf_res**2)))
        wf_mae = float(np.mean(np.abs(wf_res)))
    else:
        wf_rmse, wf_mae = rmse, mae

    latest = X[-1]
    latest_design = np.r_[1.0, latest]
    next_forecast = float(latest_design @ coef)

    ts0 = pd.Timestamp(d["release_date"].iloc[0]).to_pydatetime()
    if ts0.tzinfo is None:
        ts0 = ts0.replace(tzinfo=timezone.utc)
    ts1 = pd.Timestamp(d["release_date"].iloc[split_idx - 1]).to_pydatetime()
    if ts1.tzinfo is None:
        ts1 = ts1.replace(tzinfo=timezone.utc)

    backtest = {
        "chronological_val_fraction": val_fraction,
        "validation_start_row": int(split_idx),
        "n_total": int(n),
        "walk_forward_steps": int(len(wf_errors)),
    }

    return RegressionResult(
        prediction=next_forecast,
        rmse=rmse,
        mae=mae,
        training_start=ts0,
        training_end=ts1,
        train_rmse=train_rmse,
        train_mae=train_mae,
        n_train=int(split_idx),
        n_val=int(len(y_val)),
        target_column="cpi_mom_next",
        target_description="CPI m/m % next month (FRED CPIAUCSL when available; else proxy)",
        walk_forward_val_rmse=wf_rmse,
        walk_forward_val_mae=wf_mae,
        backtest=backtest,
    )


def mom_percent_to_yes_probability(predicted_mom_pct: float, threshold_pct: float = 0.3, scale: float = 12.0) -> float:
    """
    Map expected next-month CPI m/m (percent points) to P(YES) for 'CPI over threshold %' style contracts.
    Logistic: P = sigmoid(scale * (pred - threshold)).
    """
    x = float(scale) * (float(predicted_mom_pct) - float(threshold_pct))
    p = 1.0 / (1.0 + np.exp(-np.clip(x, -30.0, 30.0)))
    return float(max(0.01, min(0.99, p)))
