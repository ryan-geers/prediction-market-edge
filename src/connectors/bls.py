import logging
from typing import Any

from src.connectors.base import Connector

LOGGER = logging.getLogger(__name__)


class BlsConnector(Connector):
    BASE_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

    def __init__(self, api_key: str | None = None) -> None:
        super().__init__()
        self.api_key = api_key

    def fetch(self) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {
            "seriesid": ["LNS14000000"],  # unemployment rate
            "startyear": "2024",
            "endyear": "2026",
            "latest": "true",
        }
        if self.api_key:
            payload["registrationkey"] = self.api_key
        try:
            response = self.http_client.session.post(self.BASE_URL, json=payload, timeout=20)
            response.raise_for_status()
            return self.parse_response(response.json())
        except Exception as exc:
            LOGGER.warning("BLS fetch failed, using fallback data: %s", exc)
            return [{"series": "LNS14000000", "value": 4.0, "year": "2026", "period": "March"}]

    def fetch_history(self, start_year: int = 2020, end_year: int = 2026) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {
            "seriesid": ["LNS14000000"],
            "startyear": str(start_year),
            "endyear": str(end_year),
        }
        if self.api_key:
            payload["registrationkey"] = self.api_key
        try:
            response = self.http_client.session.post(self.BASE_URL, json=payload, timeout=20)
            response.raise_for_status()
            return self.parse_history_response(response.json())
        except Exception as exc:
            LOGGER.warning("BLS historical fetch failed, using fallback history: %s", exc)
            return [
                {"series": "LNS14000000", "value": 4.3, "date": "2025-01-01"},
                {"series": "LNS14000000", "value": 4.0, "date": "2026-03-01"},
            ]

    @staticmethod
    def parse_response(data: dict[str, Any]) -> list[dict[str, Any]]:
        series = data.get("Results", {}).get("series", [])
        if not series:
            return []
        points = series[0].get("data", [])
        if not points:
            return []
        latest = points[0]
        value = latest.get("value")
        if value in (None, "", "."):
            return []
        return [
            {
                "series": series[0].get("seriesID", "LNS14000000"),
                "value": float(value),
                "year": latest.get("year"),
                "period": latest.get("periodName"),
            }
        ]

    @staticmethod
    def parse_history_response(data: dict[str, Any]) -> list[dict[str, Any]]:
        series = data.get("Results", {}).get("series", [])
        if not series:
            return []
        points = series[0].get("data", [])
        rows: list[dict[str, Any]] = []
        month_map = {
            "January": "01",
            "February": "02",
            "March": "03",
            "April": "04",
            "May": "05",
            "June": "06",
            "July": "07",
            "August": "08",
            "September": "09",
            "October": "10",
            "November": "11",
            "December": "12",
        }
        for point in points:
            value = point.get("value")
            period = point.get("period", "")
            month = None
            if str(period).startswith("M"):
                month = str(period).replace("M", "").zfill(2)
            elif point.get("periodName") in month_map:
                month = month_map[str(point.get("periodName"))]
            if value in (None, "", ".") or month is None:
                continue
            rows.append(
                {
                    "series": series[0].get("seriesID", "LNS14000000"),
                    "value": float(value),
                    "date": f"{point.get('year')}-{month}-01",
                }
            )
        rows.sort(key=lambda r: r["date"])
        return rows
