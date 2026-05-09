"""
AR model for next-month U-3 unemployment rate (UNRATE / LNS14000000).

Target: predict the *level* of next month's seasonally-adjusted unemployment
rate so we can compare it against the threshold embedded in Kalshi KXU3
market tickers (e.g. KXU3-26MAY-T4.8 → threshold = 4.8).

Features: AR(3) lags + 3-month rolling trend. OLS is sufficient given the
strong serial correlation in monthly UNRATE (~0.97 autocorrelation).

Expected validation RMSE: 0.06 – 0.15 pp (well below typical Kalshi spread).
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

LOGGER = logging.getLogger(__name__)

UNRATE_SERIES = {"UNRATE", "LNS14000000"}
MIN_TRAIN_ROWS = 12  # need at least 12 months to compute lag-3 + trend


@dataclass
class UnrateRegressionResult:
    prediction: float          # predicted next-month UNRATE level (percentage points)
    rmse: float
    mae: float
    train_rmse: float
    train_mae: float
    walk_forward_val_rmse: float
    walk_forward_val_mae: float
    n_train: int
    n_val: int
    training_start: datetime
    training_end: datetime
    target_description: str = "UNRATE level next month (pp)"
    backtest: dict = field(default_factory=dict)
    model_healthy: bool = True


def build_unemployment_training_frame(
    history_rows: list[dict],
    fallback_unrate: float = 4.2,
) -> pd.DataFrame:
    """
    Build an AR training frame from `macro_history` rows.

    Reuses the same rows already fetched by FRED/BLS history fetches —
    no additional API calls required.

    Columns returned: release_date, unrate_t, unrate_lag1, unrate_lag2,
                      unrate_lag3, trend_3m, unrate_next (target).
    """
    if history_rows:
        frame = pd.DataFrame(history_rows)
        if not frame.empty and "series" in frame.columns and "date" in frame.columns:
            frame["date"] = pd.to_datetime(frame["date"], errors="coerce", utc=True)
            frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
            frame = frame.dropna(subset=["date", "value"])

            unrate_rows = frame[frame["series"].isin(UNRATE_SERIES)]
            if not unrate_rows.empty:
                series = (
                    unrate_rows.set_index("date")["value"]
                    .sort_index()
                )
                # Resample to month-start, keep the last observation per month.
                monthly = series.resample("MS").last().ffill().dropna()

                if len(monthly) >= MIN_TRAIN_ROWS + 4:
                    return _build_ar_frame(monthly)

    LOGGER.warning(
        "Insufficient UNRATE history for AR model — using synthetic fallback "
        "(fallback_unrate=%.1f). Check that FRED fetch_history includes UNRATE.",
        fallback_unrate,
    )
    return _build_synthetic_frame(fallback_unrate)


def _build_ar_frame(monthly: pd.Series) -> pd.DataFrame:
    df = pd.DataFrame({"unrate_t": monthly})
    df["unrate_lag1"] = df["unrate_t"].shift(1)
    df["unrate_lag2"] = df["unrate_t"].shift(2)
    df["unrate_lag3"] = df["unrate_t"].shift(3)
    df["trend_3m"] = df["unrate_t"].diff(3)          # 3-month change as trend signal
    df["unrate_next"] = df["unrate_t"].shift(-1)      # one-month-ahead target
    df = df.dropna()
    df = df.reset_index().rename(columns={"date": "release_date"})
    return df[["release_date", "unrate_t", "unrate_lag1", "unrate_lag2", "unrate_lag3", "trend_3m", "unrate_next"]]


def _build_synthetic_frame(base_unrate: float, periods: int = 48) -> pd.DataFrame:
    """Synthetic AR frame centred on `base_unrate` with mild mean-reversion."""
    now = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    rng = np.random.default_rng(42)
    rates: list[float] = []
    u = base_unrate
    for _ in range(periods + 4):
        u = 0.97 * u + 0.03 * base_unrate + rng.normal(0, 0.05)
        u = max(2.0, min(12.0, u))
        rates.append(u)
    rows = []
    for i in range(3, periods + 3):
        dt = now - timedelta(days=30 * (periods + 3 - i))
        rows.append(
            {
                "release_date": dt,
                "unrate_t": rates[i],
                "unrate_lag1": rates[i - 1],
                "unrate_lag2": rates[i - 2],
                "unrate_lag3": rates[i - 3],
                "trend_3m": rates[i] - rates[i - 3],
                "unrate_next": rates[i + 1],
            }
        )
    return pd.DataFrame(rows)


_FEATURE_COLS = ["unrate_t", "unrate_lag1", "unrate_lag2", "unrate_lag3", "trend_3m"]


def train_validate_predict_unemployment(
    df: pd.DataFrame,
    val_fraction: float = 0.2,
    min_train_rows: int = MIN_TRAIN_ROWS,
) -> UnrateRegressionResult:
    """
    Time-ordered OLS on AR features → next-month UNRATE level.

    Mirrors the structure of `train_validate_predict` in model.py so the
    two models are consistently evaluated.
    """
    d = df.sort_values("release_date").reset_index(drop=True)
    y = d["unrate_next"].to_numpy(dtype=float)
    X = d[_FEATURE_COLS].to_numpy(dtype=float)
    n = len(d)

    if n < min_train_rows + 2:
        raise ValueError(f"Insufficient rows for unemployment model validation (n={n})")

    split_idx = max(min_train_rows, int(n * (1.0 - val_fraction)))
    split_idx = min(split_idx, n - 1)

    X_train, X_val = X[:split_idx], X[split_idx:]
    y_train, y_val = y[:split_idx], y[split_idx:]

    train_design = np.c_[np.ones(X_train.shape[0]), X_train]
    coef, _, _, _ = np.linalg.lstsq(train_design, y_train, rcond=None)

    y_hat_train = train_design @ coef
    train_rmse = float(np.sqrt(np.mean((y_train - y_hat_train) ** 2)))
    train_mae = float(np.mean(np.abs(y_train - y_hat_train)))

    val_design = np.c_[np.ones(X_val.shape[0]), X_val]
    y_hat_val = val_design @ coef
    rmse = float(np.sqrt(np.mean((y_val - y_hat_val) ** 2)))
    mae = float(np.mean(np.abs(y_val - y_hat_val)))

    if rmse < 1e-6:
        LOGGER.warning(
            "Unemployment model: suspiciously low val RMSE (%.2e) — possible target leakage. "
            "n_train=%d, n_val=%d",
            rmse, split_idx, len(y_val),
        )

    # Walk-forward validation on the chronological tail.
    wf_errors: list[float] = []
    for k in range(len(y_val)):
        end = split_idx + k
        if end < 3:
            continue
        des = np.c_[np.ones(end), X[:end]]
        wcoef, _, _, _ = np.linalg.lstsq(des, y[:end], rcond=None)
        x_next = np.r_[1.0, X[end]]
        pred = float(x_next @ wcoef)
        wf_errors.append(y[end] - pred)

    if wf_errors:
        wf_arr = np.array(wf_errors, dtype=float)
        wf_rmse = float(np.sqrt(np.mean(wf_arr ** 2)))
        wf_mae = float(np.mean(np.abs(wf_arr)))
    else:
        wf_rmse, wf_mae = rmse, mae

    latest_design = np.r_[1.0, X[-1]]
    prediction = float(latest_design @ coef)

    model_healthy = rmse >= 1e-6 and 1.0 <= prediction <= 15.0

    ts0 = pd.Timestamp(d["release_date"].iloc[0]).to_pydatetime()
    if ts0.tzinfo is None:
        ts0 = ts0.replace(tzinfo=timezone.utc)
    ts1 = pd.Timestamp(d["release_date"].iloc[split_idx - 1]).to_pydatetime()
    if ts1.tzinfo is None:
        ts1 = ts1.replace(tzinfo=timezone.utc)

    return UnrateRegressionResult(
        prediction=prediction,
        rmse=rmse,
        mae=mae,
        train_rmse=train_rmse,
        train_mae=train_mae,
        walk_forward_val_rmse=wf_rmse,
        walk_forward_val_mae=wf_mae,
        n_train=int(split_idx),
        n_val=int(len(y_val)),
        training_start=ts0,
        training_end=ts1,
        model_healthy=model_healthy,
        backtest={
            "chronological_val_fraction": val_fraction,
            "validation_start_row": int(split_idx),
            "n_total": int(n),
            "walk_forward_steps": len(wf_errors),
        },
    )


def unrate_to_yes_probability(
    predicted_level: float,
    threshold: float,
    scale: float = 30.0,
) -> float:
    """
    P(YES) for 'unemployment above threshold %' contracts.

    Uses a steeper sigmoid than the CPI mapper (scale=30 vs 12) because
    UNRATE changes are small — a 0.1 pp miss against threshold still
    represents meaningful directional conviction.
    """
    x = scale * (predicted_level - threshold)
    p = 1.0 / (1.0 + np.exp(-np.clip(x, -30.0, 30.0)))
    return float(max(0.01, min(0.99, p)))
