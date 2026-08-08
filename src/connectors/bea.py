import logging
from typing import Any

from src.connectors.base import Connector
from src.connectors.history_utils import expand_sparse_history_to_monthly

LOGGER = logging.getLogger(__name__)

# Latest no-key stub level — must stay consistent with the end of the fallback
# history trajectory below (do not reuse the old 123.4 point-estimate stub).
_BEA_FALLBACK_LATEST_PCEPI = 132.9

# Quarterly PCEPI anchors for the no-key path. Expanded to monthly before return.
# The previous series ended 132.6 (2026-01) → 123.4 (2026-03), a leftover stub
# cliff that dominated the last CPI feature row and flipped paper decisions.
_BEA_FALLBACK_HISTORY_ANCHORS: list[dict] = [
    {"series": "PCEPI", "value": 104.2, "date": "2016-01-01"},
    {"series": "PCEPI", "value": 104.9, "date": "2016-04-01"},
    {"series": "PCEPI", "value": 105.5, "date": "2016-07-01"},
    {"series": "PCEPI", "value": 106.0, "date": "2016-10-01"},
    {"series": "PCEPI", "value": 106.7, "date": "2017-01-01"},
    {"series": "PCEPI", "value": 107.2, "date": "2017-04-01"},
    {"series": "PCEPI", "value": 107.6, "date": "2017-07-01"},
    {"series": "PCEPI", "value": 108.1, "date": "2017-10-01"},
    {"series": "PCEPI", "value": 108.9, "date": "2018-01-01"},
    {"series": "PCEPI", "value": 109.8, "date": "2018-04-01"},
    {"series": "PCEPI", "value": 110.4, "date": "2018-07-01"},
    {"series": "PCEPI", "value": 110.6, "date": "2018-10-01"},
    {"series": "PCEPI", "value": 110.8, "date": "2019-01-01"},
    {"series": "PCEPI", "value": 111.5, "date": "2019-04-01"},
    {"series": "PCEPI", "value": 111.9, "date": "2019-07-01"},
    {"series": "PCEPI", "value": 112.2, "date": "2019-10-01"},
    {"series": "PCEPI", "value": 112.6, "date": "2020-01-01"},
    {"series": "PCEPI", "value": 111.0, "date": "2020-04-01"},
    {"series": "PCEPI", "value": 112.3, "date": "2020-07-01"},
    {"series": "PCEPI", "value": 113.4, "date": "2020-10-01"},
    {"series": "PCEPI", "value": 114.0, "date": "2021-01-01"},
    {"series": "PCEPI", "value": 116.3, "date": "2021-04-01"},
    {"series": "PCEPI", "value": 118.2, "date": "2021-07-01"},
    {"series": "PCEPI", "value": 120.0, "date": "2021-10-01"},
    {"series": "PCEPI", "value": 121.3, "date": "2022-01-01"},
    {"series": "PCEPI", "value": 124.0, "date": "2022-04-01"},
    {"series": "PCEPI", "value": 125.8, "date": "2022-07-01"},
    {"series": "PCEPI", "value": 126.4, "date": "2022-10-01"},
    {"series": "PCEPI", "value": 126.5, "date": "2023-01-01"},
    {"series": "PCEPI", "value": 127.3, "date": "2023-04-01"},
    {"series": "PCEPI", "value": 127.9, "date": "2023-07-01"},
    {"series": "PCEPI", "value": 128.5, "date": "2023-10-01"},
    {"series": "PCEPI", "value": 128.8, "date": "2024-01-01"},
    {"series": "PCEPI", "value": 129.5, "date": "2024-04-01"},
    {"series": "PCEPI", "value": 130.1, "date": "2024-07-01"},
    {"series": "PCEPI", "value": 130.6, "date": "2024-10-01"},
    {"series": "PCEPI", "value": 131.0, "date": "2025-01-01"},
    {"series": "PCEPI", "value": 131.5, "date": "2025-04-01"},
    {"series": "PCEPI", "value": 131.9, "date": "2025-07-01"},
    {"series": "PCEPI", "value": 132.3, "date": "2025-10-01"},
    {"series": "PCEPI", "value": 132.6, "date": "2026-01-01"},
    {"series": "PCEPI", "value": _BEA_FALLBACK_LATEST_PCEPI, "date": "2026-04-01"},
]


class BeaConnector(Connector):
    BASE_URL = "https://apps.bea.gov/api/data"

    def __init__(self, api_key: str | None = None) -> None:
        super().__init__()
        self.api_key = api_key

    def fetch(self) -> list[dict[str, Any]]:
        if not self.api_key:
            return [{"series": "PCEPI", "value": _BEA_FALLBACK_LATEST_PCEPI, "period": "2026M04"}]

        params = {
            "UserID": self.api_key,
            "method": "GetData",
            "datasetname": "NIPA",
            "TableName": "T20805",
            "LineNumber": "1",
            "Frequency": "M",
            "Year": "X",
            "ResultFormat": "JSON",
        }
        try:
            data = self.http_client.get_json(self.BASE_URL, params=params)
            parsed = self.parse_response(data)
            if parsed:
                return parsed
            return []
        except Exception as exc:
            LOGGER.warning("BEA fetch failed, using fallback data: %s", exc)
            return [{"series": "PCEPI", "value": _BEA_FALLBACK_LATEST_PCEPI, "period": "2026M04"}]

    def fetch_history(self) -> list[dict[str, Any]]:
        if not self.api_key:
            # ~10 years of PCEPI levels so build_training_frame_from_history
            # has enough non-NaN rows to satisfy the ≥ 8 training-row requirement
            # without falling back to the fully synthetic training frame.
            # Anchors are quarterly; expand to monthly so ffill does not invent
            # flat CPI feature stretches / cliff artifacts at the series tip.
            return expand_sparse_history_to_monthly(_BEA_FALLBACK_HISTORY_ANCHORS)
        params = {
            "UserID": self.api_key,
            "method": "GetData",
            "datasetname": "NIPA",
            "TableName": "T20805",
            "LineNumber": "1",
            "Frequency": "M",
            "Year": "X",
            "ResultFormat": "JSON",
        }
        try:
            data = self.http_client.get_json(self.BASE_URL, params=params)
            return self.parse_history_response(data)
        except Exception as exc:
            LOGGER.warning("BEA historical fetch failed, using fallback history: %s", exc)
            return expand_sparse_history_to_monthly(
                [
                    {"series": "PCEPI", "value": 128.8, "date": "2024-01-01"},
                    {"series": "PCEPI", "value": 130.6, "date": "2024-10-01"},
                    {"series": "PCEPI", "value": 131.9, "date": "2025-07-01"},
                    {"series": "PCEPI", "value": _BEA_FALLBACK_LATEST_PCEPI, "date": "2026-04-01"},
                ]
            )

    @staticmethod
    def parse_response(data: dict[str, Any]) -> list[dict[str, Any]]:
        rows = data.get("BEAAPI", {}).get("Results", {}).get("Data", [])
        if not rows:
            return []
        latest = rows[-1]
        raw_val = str(latest.get("DataValue", "")).replace(",", "")
        if not raw_val:
            return []
        return [{"series": "PCEPI", "value": float(raw_val), "period": latest.get("TimePeriod")}]

    @staticmethod
    def parse_history_response(data: dict[str, Any]) -> list[dict[str, Any]]:
        rows = data.get("BEAAPI", {}).get("Results", {}).get("Data", [])
        out: list[dict[str, Any]] = []
        for row in rows:
            raw_val = str(row.get("DataValue", "")).replace(",", "")
            period = str(row.get("TimePeriod", ""))
            if not raw_val or len(period) != 7 or "M" not in period:
                continue
            year, month = period.split("M")
            out.append(
                {
                    "series": "PCEPI",
                    "value": float(raw_val),
                    "date": f"{year}-{month.zfill(2)}-01",
                }
            )
        out.sort(key=lambda r: r["date"])
        return out
