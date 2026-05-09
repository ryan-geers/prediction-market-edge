import logging
from typing import Any

from src.connectors.base import Connector

LOGGER = logging.getLogger(__name__)


class FredConnector(Connector):
    BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

    def __init__(self, api_key: str | None = None) -> None:
        super().__init__()
        self.api_key = api_key

    def _fetch_series(self, series_id: str) -> float | None:
        params = {
            "series_id": series_id,
            "api_key": self.api_key or "",
            "file_type": "json",
            "sort_order": "desc",
            "limit": 1,
        }
        payload = self.http_client.get_json(self.BASE_URL, params=params)
        return self.parse_latest_value(payload)

    def fetch_series_history(self, series_id: str, limit: int = 1200) -> list[dict[str, Any]]:
        params = {
            "series_id": series_id,
            "api_key": self.api_key or "",
            "file_type": "json",
            "sort_order": "desc",  # most-recent first so limit selects recent data, not 1913-era data
            "limit": limit,        # 1200 months (~100 years) covers the full PCEPI history from 1959
        }
        payload = self.http_client.get_json(self.BASE_URL, params=params)
        observations = payload.get("observations", [])
        rows: list[dict[str, Any]] = []
        for obs in observations:
            value = obs.get("value")
            if value in (None, ".", ""):
                continue
            rows.append(
                {
                    "series": series_id,
                    "value": float(value),
                    "date": obs.get("date"),
                    "vintage_date": obs.get("realtime_start"),
                }
            )
        return rows

    @staticmethod
    def parse_latest_value(payload: dict[str, Any]) -> float | None:
        observations = payload.get("observations", [])
        if not observations:
            return None
        latest = observations[0].get("value")
        if latest in (None, ".", ""):
            return None
        return float(latest)

    def fetch(self) -> list[dict[str, Any]]:
        try:
            ppi = self._fetch_series("PPIACO")
            unrate = self._fetch_series("UNRATE")
            rows = []
            if ppi is not None:
                rows.append({"series": "PPIACO", "value": ppi})
            if unrate is not None:
                rows.append({"series": "UNRATE", "value": unrate})
            if rows:
                return rows
        except Exception as exc:
            LOGGER.warning("FRED fetch failed, using fallback data: %s", exc)
        return [{"series": "PPIACO", "value": 245.1}, {"series": "UNRATE", "value": 4.0}]

    def fetch_history(self) -> list[dict[str, Any]]:
        try:
            rows = (
                self.fetch_series_history("PPIACO")
                + self.fetch_series_history("UNRATE")
                + self.fetch_series_history("CPIAUCSL")
            )
            if rows:
                return rows
        except Exception as exc:
            LOGGER.warning("FRED historical fetch failed, using fallback history: %s", exc)
        # Synthetic fallback: index levels for CPI mom construction (aligned monthly dates)
        return [
            {"series": "PPIACO", "value": 242.0, "date": "2025-01-01", "vintage_date": "2025-02-01"},
            {"series": "PPIACO", "value": 245.1, "date": "2026-03-01", "vintage_date": "2026-04-01"},
            {"series": "UNRATE", "value": 4.2, "date": "2025-01-01", "vintage_date": "2025-02-01"},
            {"series": "UNRATE", "value": 4.0, "date": "2026-03-01", "vintage_date": "2026-04-01"},
            {"series": "CPIAUCSL", "value": 310.0, "date": "2025-01-01", "vintage_date": "2025-02-01"},
            {"series": "CPIAUCSL", "value": 311.2, "date": "2025-02-01", "vintage_date": "2025-03-01"},
            {"series": "CPIAUCSL", "value": 312.5, "date": "2025-03-01", "vintage_date": "2025-04-01"},
            {"series": "CPIAUCSL", "value": 313.0, "date": "2026-03-01", "vintage_date": "2026-04-01"},
        ]
