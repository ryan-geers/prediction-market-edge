import logging
import re
from typing import Any

from src.connectors.base import Connector

LOGGER = logging.getLogger(__name__)

# Confirmed live against Kalshi Trade API v2.
UNRATE_SERIES = {"KXU3", "KXECONSTATU3"}
CPI_SERIES = {"KXCPI", "KXMCPI", "CPIM"}

# Ticker-pattern classifiers used when series_ticker isn't available
# (e.g. from a generic /markets fetch).
_UNRATE_PATTERN = re.compile(r"^(KXU3|KXECONSTATU3)-", re.IGNORECASE)
_CPI_PATTERN = re.compile(r"(CPI|KXCPI|CPIM)", re.IGNORECASE)

# Kalshi series tickers to try for each contract type.  First hit wins.
_CPI_SERIES_CANDIDATES = ["KXCPI", "KXMCPI", "CPIM", "CPI"]
_UNRATE_SERIES_CANDIDATES = ["KXU3", "KXECONSTATU3"]


def _classify_series(series_ticker: str) -> str:
    if series_ticker in UNRATE_SERIES:
        return "unemployment"
    if series_ticker in CPI_SERIES:
        return "cpi"
    return "unknown"


def _classify_by_ticker(ticker: str) -> str:
    """Classify a market by its ticker string when no series_ticker is available."""
    if _UNRATE_PATTERN.match(ticker):
        return "unemployment"
    if _CPI_PATTERN.search(ticker):
        return "cpi"
    return "unknown"


def _parse_threshold(ticker: str) -> float | None:
    """
    Extract the numeric threshold from a Kalshi ticker.

    Handles two formats:
      KXU3-26MAY-T4.8        → 4.8   (unemployment: -T suffix)
      CPI-MAY-OVER-0.3       → 0.3   (CPI: OVER- suffix)
    """
    # Unemployment / generic -T format
    if "-T" in ticker:
        try:
            return float(ticker.rsplit("-T", 1)[-1])
        except ValueError:
            pass
    # CPI OVER- format
    m = re.search(r"OVER-([0-9.]+)", ticker, re.IGNORECASE)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            pass
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

        # Prefer the explicitly-passed series_ticker; fall back to the API field;
        # finally infer from the ticker string itself.
        resolved_series = (
            series_ticker
            or item.get("series_ticker", "")
            or ""
        )
        contract_type = (
            _classify_series(resolved_series)
            if resolved_series
            else _classify_by_ticker(ticker)
        )

        return {
            "venue": "kalshi",
            "contract_id": ticker,
            "label": item.get("title", "Kalshi market"),
            "best_bid": bid,
            "best_ask": ask,
            "last_trade": last,
            "series_ticker": resolved_series,
            "contract_type": contract_type,
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

    def _get_markets(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        """Low-level GET /markets call; returns raw normalized list or []."""
        series_ticker = params.get("series_ticker", "")
        try:
            response = self.http_client.session.get(
                f"{self.BASE_URL}/markets",
                params=params,
                headers=self._auth_headers(),
                timeout=self.http_client.timeout_seconds,
            )
            response.raise_for_status()
            return self.parse_markets(response.json(), series_ticker=series_ticker)
        except Exception as exc:
            LOGGER.warning("Kalshi GET /markets failed (params=%s): %s", params, exc)
            return []

    def fetch_series(self, series_ticker: str) -> list[dict[str, Any]]:
        """Fetch all open markets for a single Kalshi series ticker."""
        markets = self._get_markets({"series_ticker": series_ticker, "status": "open", "limit": 100})
        if not markets:
            LOGGER.warning("Kalshi: 0 open markets for series=%s", series_ticker)
        return markets

    def fetch_markets(self, series_tickers: list[str]) -> list[dict[str, Any]]:
        """
        Fetch open markets across a list of series tickers.

        Fallback chain:
          1. Series-specific fetches (works best with API key).
          2. Generic open-market fetch filtered by contract type pattern
             (works unauthenticated; returns whatever Kalshi's top-200 has).
          3. Hard-coded stubs so the pipeline never fails cold.
        """
        seen: set[str] = set()
        all_markets: list[dict[str, Any]] = []
        for st in series_tickers:
            for m in self.fetch_series(st):
                if m["contract_id"] not in seen:
                    seen.add(m["contract_id"])
                    all_markets.append(m)

        if all_markets:
            LOGGER.info(
                "Kalshi: %d markets from series fetches (%s)",
                len(all_markets),
                ", ".join(f"{m['contract_id']}" for m in all_markets),
            )
            return all_markets

        # --- Fallback 1: generic fetch, filter by contract type ---
        LOGGER.warning(
            "Kalshi: series-specific fetches returned nothing — trying generic fetch "
            "(tip: set KALSHI_API_KEY to get full series coverage)"
        )
        generic = self._get_markets({"status": "open", "limit": 200})
        relevant = [m for m in generic if m["contract_type"] in {"cpi", "unemployment"}]
        if relevant:
            LOGGER.info(
                "Kalshi generic fallback: %d relevant markets (%s CPI, %s unemployment)",
                len(relevant),
                sum(1 for m in relevant if m["contract_type"] == "cpi"),
                sum(1 for m in relevant if m["contract_type"] == "unemployment"),
            )
            return relevant

        # --- Fallback 2: hard-coded stubs ---
        LOGGER.warning("Kalshi: generic fetch also returned nothing — using hard-coded fallback stubs")
        return self._fallback_stubs()

    @staticmethod
    def _fallback_stubs() -> list[dict[str, Any]]:
        # is_stub=True is propagated to decision_reason so the digest can show
        # a loud warning whenever the pipeline runs on synthetic prices.
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
                "is_stub": True,
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
                "is_stub": True,
            },
        ]

    def fetch(self) -> list[dict[str, Any]]:
        """Legacy single-series fetch kept for backward compatibility."""
        return self.fetch_markets(["KXCPI", "KXU3"])
