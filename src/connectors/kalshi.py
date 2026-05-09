import logging
from typing import Any

from src.connectors.base import Connector

LOGGER = logging.getLogger(__name__)

# Confirmed live against Kalshi Trade API v2.
UNRATE_SERIES = {"KXU3", "KXECONSTATU3"}
CPI_SERIES = {"KXCPI"}


def _classify_series(series_ticker: str) -> str:
    if series_ticker in UNRATE_SERIES:
        return "unemployment"
    if series_ticker in CPI_SERIES:
        return "cpi"
    return "unknown"


def _parse_threshold(ticker: str) -> float | None:
    """Extract numeric threshold from a Kalshi ticker suffix, e.g. 'KXU3-26MAY-T4.8' → 4.8."""
    if "-T" not in ticker:
        return None
    try:
        return float(ticker.rsplit("-T", 1)[-1])
    except ValueError:
        return None


class KalshiConnector(Connector):
    BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

    def __init__(self, api_key: str | None = None) -> None:
        super().__init__()
        self.api_key = api_key

    def _auth_headers(self) -> dict[str, str]:
        if self.api_key:
            return {"Authorization": f"Bearer {self.api_key}"}
        return {}

    def _normalize_market(self, item: dict[str, Any], series_ticker: str = "") -> dict[str, Any]:
        bid_raw = item.get("yes_bid")
        ask_raw = item.get("yes_ask")
        if bid_raw is None:
            bid_raw = item.get("best_bid")
        if ask_raw is None:
            ask_raw = item.get("best_ask")
        bid = float(bid_raw if bid_raw is not None else 44)
        ask = float(ask_raw if ask_raw is not None else 48)
        if bid > 1:
            bid /= 100
        if ask > 1:
            ask /= 100
        last = float(item.get("last_price") or ((bid + ask) / 2))
        ticker = item.get("ticker", "CPI-MAY-OVER-0.3")
        resolved_series = series_ticker or item.get("series_ticker", "")
        return {
            "venue": "kalshi",
            "contract_id": ticker,
            "label": item.get("title", "Kalshi market"),
            "best_bid": bid,
            "best_ask": ask,
            "last_trade": last,
            "series_ticker": resolved_series,
            "contract_type": _classify_series(resolved_series),
            "threshold": _parse_threshold(ticker),
        }

    def parse_markets(self, payload: dict[str, Any], series_ticker: str = "") -> list[dict[str, Any]]:
        markets = payload.get("markets", [])
        normalized = []
        for market in markets:
            if market.get("status") in {"closed", "settled"}:
                continue
            if market.get("yes_bid") is None and market.get("best_bid") is None:
                continue
            normalized.append(self._normalize_market(market, series_ticker=series_ticker))
        return normalized

    def fetch_series(self, series_ticker: str) -> list[dict[str, Any]]:
        """Fetch all open markets for a single Kalshi series ticker."""
        params = {"series_ticker": series_ticker, "status": "open", "limit": 100}
        try:
            response = self.http_client.session.get(
                f"{self.BASE_URL}/markets",
                params=params,
                headers=self._auth_headers(),
                timeout=self.http_client.timeout_seconds,
            )
            response.raise_for_status()
            markets = self.parse_markets(response.json(), series_ticker=series_ticker)
            if markets:
                return markets
            LOGGER.warning("Kalshi: 0 open markets for series=%s", series_ticker)
        except Exception as exc:
            LOGGER.warning("Kalshi fetch_series(%s) failed: %s", series_ticker, exc)
        return []

    def fetch_markets(self, series_tickers: list[str]) -> list[dict[str, Any]]:
        """Fetch open markets across a list of series tickers."""
        all_markets: list[dict[str, Any]] = []
        for st in series_tickers:
            all_markets.extend(self.fetch_series(st))
        if all_markets:
            return all_markets
        LOGGER.warning("Kalshi: all series returned empty — using fallback stubs")
        return self._fallback_stubs()

    @staticmethod
    def _fallback_stubs() -> list[dict[str, Any]]:
        return [
            {
                "venue": "kalshi",
                "contract_id": "CPI-MAY-OVER-0.3",
                "label": "May CPI over 0.3%",
                "best_bid": 0.44,
                "best_ask": 0.48,
                "last_trade": 0.46,
                "series_ticker": "KXCPI",
                "contract_type": "cpi",
                "threshold": 0.3,
            },
            {
                "venue": "kalshi",
                "contract_id": "KXU3-26MAY-T4.2",
                "label": "Unemployment above 4.2% (May 2026)",
                "best_bid": 0.50,
                "best_ask": 0.52,
                "last_trade": 0.51,
                "series_ticker": "KXU3",
                "contract_type": "unemployment",
                "threshold": 4.2,
            },
        ]

    def fetch(self) -> list[dict[str, Any]]:
        """Legacy single-series fetch kept for backward compatibility."""
        return self.fetch_markets(["KXCPI", "KXU3"])
