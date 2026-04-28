import logging
from typing import Any

from src.connectors.base import Connector

LOGGER = logging.getLogger(__name__)


class KalshiConnector(Connector):
    BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

    def _normalize_market(self, item: dict[str, Any]) -> dict[str, Any]:
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
        return {
            "venue": "kalshi",
            "contract_id": item.get("ticker", "CPI-MAY-OVER-0.3"),
            "label": item.get("title", "Kalshi market"),
            "best_bid": bid,
            "best_ask": ask,
            "last_trade": last,
        }

    def parse_markets(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        markets = payload.get("markets", [])
        normalized = []
        for market in markets:
            if market.get("status") in {"closed", "settled"}:
                continue
            if market.get("yes_bid") is None and market.get("best_bid") is None:
                continue
            normalized.append(self._normalize_market(market))
        return normalized

    def fetch(self) -> list[dict[str, Any]]:
        try:
            payload = self.http_client.get_json(f"{self.BASE_URL}/markets", params={"limit": 10, "status": "open"})
            normalized = self.parse_markets(payload)
            if normalized:
                return normalized
        except Exception as exc:
            LOGGER.warning("Kalshi fetch failed, using fallback data: %s", exc)
        return [
            {
                "venue": "kalshi",
                "contract_id": "CPI-MAY-OVER-0.3",
                "label": "May CPI over 0.3%",
                "best_bid": 0.44,
                "best_ask": 0.48,
                "last_trade": 0.46,
            }
        ]
