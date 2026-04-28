import logging
from typing import Any

from src.connectors.base import Connector

LOGGER = logging.getLogger(__name__)


class PolymarketConnector(Connector):
    BASE_URL = "https://gamma-api.polymarket.com/markets"

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value in (None, "", "."):
            return None
        return float(value)

    def parse_markets(self, payload: Any) -> list[dict[str, Any]]:
        if not isinstance(payload, list):
            return []
        rows = []
        for item in payload:
            if item.get("closed") is True:
                continue
            bid = self._to_float(item.get("bestBid"))
            ask = self._to_float(item.get("bestAsk"))
            last = self._to_float(item.get("lastTradePrice"))
            if bid is None and ask is None and last is None:
                continue
            if bid is None and ask is not None:
                bid = max(0.0, ask - 0.02)
            if ask is None and bid is not None:
                ask = min(1.0, bid + 0.02)
            if last is None and bid is not None and ask is not None:
                last = (bid + ask) / 2
            rows.append(
                {
                    "venue": "polymarket",
                    "contract_id": str(item.get("conditionId", item.get("id", "unknown"))),
                    "label": item.get("question", "Polymarket market"),
                    "best_bid": max(0.0, float(bid or 0.0)),
                    "best_ask": min(1.0, float(ask or 1.0)),
                    "last_trade": float(last or 0.5),
                }
            )
        return rows

    def fetch(self) -> list[dict[str, Any]]:
        try:
            payload = self.http_client.get_json(self.BASE_URL, params={"limit": 10, "closed": "false"})
            return self.parse_markets(payload)
        except Exception as exc:
            LOGGER.warning("Polymarket fetch failed: %s", exc)
            return []
