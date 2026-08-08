"""Helpers for connector synthetic / fallback macro histories."""

from __future__ import annotations

from typing import Any

import pandas as pd


def expand_sparse_history_to_monthly(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Expand per-series sparse (e.g. quarterly) level histories to calendar-month
    frequency via time interpolation.

    ``build_training_frame_from_history`` resamples to month-start and forward-fills
    before computing CPI m/m labels. Quarterly anchors therefore produce two exact
    zero MoM labels between every real print, which collapses OLS training and can
    flip paper-trade decisions when API keys are missing or FRED/BEA fail over.
    """
    if not rows:
        return []

    by_series: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        series = str(row.get("series", ""))
        if not series:
            continue
        by_series.setdefault(series, []).append(row)

    out: list[dict[str, Any]] = []
    for series, points in by_series.items():
        series_values = pd.Series(
            {pd.Timestamp(p["date"]): float(p["value"]) for p in points if p.get("date") is not None}
        ).sort_index()
        if series_values.empty:
            continue
        if len(series_values) == 1:
            dt = series_values.index[0]
            out.append(
                {
                    "series": series,
                    "value": float(series_values.iloc[0]),
                    "date": dt.strftime("%Y-%m-%d"),
                }
            )
            continue
        monthly = series_values.resample("MS").interpolate("time")
        for dt, value in monthly.items():
            if pd.isna(value):
                continue
            out.append(
                {
                    "series": series,
                    "value": float(value),
                    "date": pd.Timestamp(dt).strftime("%Y-%m-%d"),
                }
            )
    out.sort(key=lambda r: (r["series"], r["date"]))
    return out
