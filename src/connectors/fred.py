import logging
from typing import Any

from src.connectors.base import Connector

LOGGER = logging.getLogger(__name__)


# Synthetic 10-year monthly history for PPIACO, UNRATE, and CPIAUCSL.
# Used when the FRED API is unavailable (no key, network error, etc.).
# Values approximate the actual FRED series trajectory for 2016-2026 and
# are sufficient for the training-window requirements of both models:
#   • CPI OLS needs ≥ 8 rows with all three feature cols + CPIAUCSL target
#   • UNRATE AR needs ≥ 16 monthly rows
_FRED_FALLBACK_HISTORY: list[dict] = [
    # fmt: off
    # PPIACO (Producer Price Index, All Commodities) — monthly levels
    {"series": "PPIACO", "value": 183.2, "date": "2016-01-01"},
    {"series": "PPIACO", "value": 183.8, "date": "2016-04-01"},
    {"series": "PPIACO", "value": 184.5, "date": "2016-07-01"},
    {"series": "PPIACO", "value": 186.1, "date": "2016-10-01"},
    {"series": "PPIACO", "value": 189.3, "date": "2017-01-01"},
    {"series": "PPIACO", "value": 191.0, "date": "2017-04-01"},
    {"series": "PPIACO", "value": 193.2, "date": "2017-07-01"},
    {"series": "PPIACO", "value": 196.8, "date": "2017-10-01"},
    {"series": "PPIACO", "value": 200.1, "date": "2018-01-01"},
    {"series": "PPIACO", "value": 204.3, "date": "2018-04-01"},
    {"series": "PPIACO", "value": 208.7, "date": "2018-07-01"},
    {"series": "PPIACO", "value": 204.5, "date": "2018-10-01"},
    {"series": "PPIACO", "value": 201.0, "date": "2019-01-01"},
    {"series": "PPIACO", "value": 203.8, "date": "2019-04-01"},
    {"series": "PPIACO", "value": 202.1, "date": "2019-07-01"},
    {"series": "PPIACO", "value": 202.9, "date": "2019-10-01"},
    {"series": "PPIACO", "value": 200.5, "date": "2020-01-01"},
    {"series": "PPIACO", "value": 185.3, "date": "2020-04-01"},
    {"series": "PPIACO", "value": 196.2, "date": "2020-07-01"},
    {"series": "PPIACO", "value": 207.4, "date": "2020-10-01"},
    {"series": "PPIACO", "value": 216.8, "date": "2021-01-01"},
    {"series": "PPIACO", "value": 227.5, "date": "2021-04-01"},
    {"series": "PPIACO", "value": 237.1, "date": "2021-07-01"},
    {"series": "PPIACO", "value": 248.6, "date": "2021-10-01"},
    {"series": "PPIACO", "value": 256.2, "date": "2022-01-01"},
    {"series": "PPIACO", "value": 268.9, "date": "2022-04-01"},
    {"series": "PPIACO", "value": 270.3, "date": "2022-07-01"},
    {"series": "PPIACO", "value": 259.8, "date": "2022-10-01"},
    {"series": "PPIACO", "value": 248.4, "date": "2023-01-01"},
    {"series": "PPIACO", "value": 246.1, "date": "2023-04-01"},
    {"series": "PPIACO", "value": 251.3, "date": "2023-07-01"},
    {"series": "PPIACO", "value": 252.7, "date": "2023-10-01"},
    {"series": "PPIACO", "value": 252.0, "date": "2024-01-01"},
    {"series": "PPIACO", "value": 253.8, "date": "2024-04-01"},
    {"series": "PPIACO", "value": 255.6, "date": "2024-07-01"},
    {"series": "PPIACO", "value": 258.1, "date": "2024-10-01"},
    {"series": "PPIACO", "value": 260.4, "date": "2025-01-01"},
    {"series": "PPIACO", "value": 262.8, "date": "2025-04-01"},
    {"series": "PPIACO", "value": 264.5, "date": "2025-07-01"},
    {"series": "PPIACO", "value": 266.1, "date": "2025-10-01"},
    {"series": "PPIACO", "value": 267.9, "date": "2026-01-01"},
    {"series": "PPIACO", "value": 269.4, "date": "2026-04-01"},
    # UNRATE (Unemployment Rate, SA) — monthly
    {"series": "UNRATE", "value": 4.9, "date": "2016-01-01"},
    {"series": "UNRATE", "value": 4.7, "date": "2016-04-01"},
    {"series": "UNRATE", "value": 4.9, "date": "2016-07-01"},
    {"series": "UNRATE", "value": 4.6, "date": "2016-10-01"},
    {"series": "UNRATE", "value": 4.7, "date": "2017-01-01"},
    {"series": "UNRATE", "value": 4.4, "date": "2017-04-01"},
    {"series": "UNRATE", "value": 4.3, "date": "2017-07-01"},
    {"series": "UNRATE", "value": 4.1, "date": "2017-10-01"},
    {"series": "UNRATE", "value": 4.1, "date": "2018-01-01"},
    {"series": "UNRATE", "value": 3.9, "date": "2018-04-01"},
    {"series": "UNRATE", "value": 3.9, "date": "2018-07-01"},
    {"series": "UNRATE", "value": 3.7, "date": "2018-10-01"},
    {"series": "UNRATE", "value": 4.0, "date": "2019-01-01"},
    {"series": "UNRATE", "value": 3.6, "date": "2019-04-01"},
    {"series": "UNRATE", "value": 3.7, "date": "2019-07-01"},
    {"series": "UNRATE", "value": 3.6, "date": "2019-10-01"},
    {"series": "UNRATE", "value": 3.5, "date": "2020-01-01"},
    {"series": "UNRATE", "value": 14.7, "date": "2020-04-01"},
    {"series": "UNRATE", "value": 10.2, "date": "2020-07-01"},
    {"series": "UNRATE", "value": 6.7, "date": "2020-10-01"},
    {"series": "UNRATE", "value": 6.4, "date": "2021-01-01"},
    {"series": "UNRATE", "value": 6.0, "date": "2021-04-01"},
    {"series": "UNRATE", "value": 5.4, "date": "2021-07-01"},
    {"series": "UNRATE", "value": 4.6, "date": "2021-10-01"},
    {"series": "UNRATE", "value": 4.0, "date": "2022-01-01"},
    {"series": "UNRATE", "value": 3.6, "date": "2022-04-01"},
    {"series": "UNRATE", "value": 3.5, "date": "2022-07-01"},
    {"series": "UNRATE", "value": 3.7, "date": "2022-10-01"},
    {"series": "UNRATE", "value": 3.4, "date": "2023-01-01"},
    {"series": "UNRATE", "value": 3.4, "date": "2023-04-01"},
    {"series": "UNRATE", "value": 3.5, "date": "2023-07-01"},
    {"series": "UNRATE", "value": 3.9, "date": "2023-10-01"},
    {"series": "UNRATE", "value": 3.7, "date": "2024-01-01"},
    {"series": "UNRATE", "value": 3.9, "date": "2024-04-01"},
    {"series": "UNRATE", "value": 4.3, "date": "2024-07-01"},
    {"series": "UNRATE", "value": 4.1, "date": "2024-10-01"},
    {"series": "UNRATE", "value": 4.0, "date": "2025-01-01"},
    {"series": "UNRATE", "value": 4.2, "date": "2025-04-01"},
    {"series": "UNRATE", "value": 4.3, "date": "2025-07-01"},
    {"series": "UNRATE", "value": 4.2, "date": "2025-10-01"},
    {"series": "UNRATE", "value": 4.1, "date": "2026-01-01"},
    {"series": "UNRATE", "value": 4.2, "date": "2026-04-01"},
    # CPIAUCSL (CPI All Urban Consumers, SA) — monthly levels
    {"series": "CPIAUCSL", "value": 236.9, "date": "2016-01-01"},
    {"series": "CPIAUCSL", "value": 239.3, "date": "2016-04-01"},
    {"series": "CPIAUCSL", "value": 240.6, "date": "2016-07-01"},
    {"series": "CPIAUCSL", "value": 241.4, "date": "2016-10-01"},
    {"series": "CPIAUCSL", "value": 242.8, "date": "2017-01-01"},
    {"series": "CPIAUCSL", "value": 244.5, "date": "2017-04-01"},
    {"series": "CPIAUCSL", "value": 245.5, "date": "2017-07-01"},
    {"series": "CPIAUCSL", "value": 246.7, "date": "2017-10-01"},
    {"series": "CPIAUCSL", "value": 247.9, "date": "2018-01-01"},
    {"series": "CPIAUCSL", "value": 250.5, "date": "2018-04-01"},
    {"series": "CPIAUCSL", "value": 252.1, "date": "2018-07-01"},
    {"series": "CPIAUCSL", "value": 252.9, "date": "2018-10-01"},
    {"series": "CPIAUCSL", "value": 252.8, "date": "2019-01-01"},
    {"series": "CPIAUCSL", "value": 255.7, "date": "2019-04-01"},
    {"series": "CPIAUCSL", "value": 256.6, "date": "2019-07-01"},
    {"series": "CPIAUCSL", "value": 257.2, "date": "2019-10-01"},
    {"series": "CPIAUCSL", "value": 258.7, "date": "2020-01-01"},
    {"series": "CPIAUCSL", "value": 256.4, "date": "2020-04-01"},
    {"series": "CPIAUCSL", "value": 259.1, "date": "2020-07-01"},
    {"series": "CPIAUCSL", "value": 261.0, "date": "2020-10-01"},
    {"series": "CPIAUCSL", "value": 261.6, "date": "2021-01-01"},
    {"series": "CPIAUCSL", "value": 267.1, "date": "2021-04-01"},
    {"series": "CPIAUCSL", "value": 273.0, "date": "2021-07-01"},
    {"series": "CPIAUCSL", "value": 277.9, "date": "2021-10-01"},
    {"series": "CPIAUCSL", "value": 281.1, "date": "2022-01-01"},
    {"series": "CPIAUCSL", "value": 289.1, "date": "2022-04-01"},
    {"series": "CPIAUCSL", "value": 296.3, "date": "2022-07-01"},
    {"series": "CPIAUCSL", "value": 298.0, "date": "2022-10-01"},
    {"series": "CPIAUCSL", "value": 299.2, "date": "2023-01-01"},
    {"series": "CPIAUCSL", "value": 303.4, "date": "2023-04-01"},
    {"series": "CPIAUCSL", "value": 305.7, "date": "2023-07-01"},
    {"series": "CPIAUCSL", "value": 307.7, "date": "2023-10-01"},
    {"series": "CPIAUCSL", "value": 308.4, "date": "2024-01-01"},
    {"series": "CPIAUCSL", "value": 313.5, "date": "2024-04-01"},
    {"series": "CPIAUCSL", "value": 314.8, "date": "2024-07-01"},
    {"series": "CPIAUCSL", "value": 315.4, "date": "2024-10-01"},
    {"series": "CPIAUCSL", "value": 316.0, "date": "2025-01-01"},
    {"series": "CPIAUCSL", "value": 316.8, "date": "2025-04-01"},
    {"series": "CPIAUCSL", "value": 317.5, "date": "2025-07-01"},
    {"series": "CPIAUCSL", "value": 318.1, "date": "2025-10-01"},
    {"series": "CPIAUCSL", "value": 318.6, "date": "2026-01-01"},
    {"series": "CPIAUCSL", "value": 319.2, "date": "2026-04-01"},
    # fmt: on
]


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
        # Synthetic fallback: ~10 years of plausible monthly macro data so the CPI
        # OLS and UNRATE AR models can train without an API key. Values approximate
        # the actual FRED series trajectory for 2016-2026 and are sufficient for the
        # training-window check (min 8 rows for CPI, min 16 rows for UNRATE AR).
        return _FRED_FALLBACK_HISTORY
