import base64
import logging
import re
import time
from typing import Any

from src.connectors.base import Connector

LOGGER = logging.getLogger(__name__)

# Kalshi main trading API — economic/financial markets (CPI, unemployment, Fed, etc.)
# NOT api.elections.kalshi.com, which only serves political/election contracts.
_BASE_URL = "https://api.kalshi.com/trade-api/v2"

# Confirmed series tickers on api.kalshi.com
UNRATE_SERIES = {"KXU3", "KXECONSTATU3"}
CPI_SERIES = {"KXCPI", "KXMCPI", "CPIM"}

# Ticker-pattern classifiers used when series_ticker isn't explicitly available.
_UNRATE_PATTERN = re.compile(r"^(KXU3|KXECONSTATU3)-", re.IGNORECASE)
_CPI_PATTERN = re.compile(r"(CPI|KXCPI|CPIM)", re.IGNORECASE)

# Ordered lists of series ticker candidates to try for each contract type.
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
    if "-T" in ticker:
        try:
            return float(ticker.rsplit("-T", 1)[-1])
        except ValueError:
            pass
    m = re.search(r"OVER-([0-9.]+)", ticker, re.IGNORECASE)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            pass
    return None


def _build_rsa_signature(private_key_pem: str, timestamp_ms: str, method: str, path: str) -> str:
    """
    Sign `timestamp_ms + method.upper() + path` with the RSA private key using
    PKCS#1 v1.5 / SHA-256, as required by api.kalshi.com.

    Returns the Base64-encoded signature string.
    """
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding

    pem_bytes = private_key_pem.encode() if isinstance(private_key_pem, str) else private_key_pem
    private_key = serialization.load_pem_private_key(pem_bytes, password=None)
    message = (timestamp_ms + method.upper() + path).encode()
    signature = private_key.sign(message, padding.PKCS1v15(), hashes.SHA256())
    return base64.b64encode(signature).decode()


class KalshiConnector(Connector):
    BASE_URL = _BASE_URL

    def __init__(self, api_key: str | None = None, key_id: str | None = None) -> None:
        super().__init__()
        self.api_key = api_key        # PEM-encoded RSA private key
        self.key_id = key_id          # Key ID UUID from Kalshi dashboard

    def _auth_headers(self, method: str = "GET", path: str = "") -> dict[str, str]:
        """
        Build Kalshi RSA auth headers for a specific request.

        Kalshi requires:
          KALSHI-ACCESS-KEY       — the key ID UUID
          KALSHI-ACCESS-SIGNATURE — Base64(RSA-SHA256(timestamp_ms + METHOD + /path))
          KALSHI-ACCESS-TIMESTAMP — current Unix time in milliseconds (string)

        Returns empty dict if credentials are not configured (falls back to
        unauthenticated, which works for public read-only endpoints).
        """
        if not self.api_key or not self.key_id:
            if self.api_key or self.key_id:
                # One half of the pair is present but not both — warn loudly.
                LOGGER.warning(
                    "Kalshi auth: %s is set but %s is missing — cannot sign requests. "
                    "Set both KALSHI_API_KEY (PEM private key) and KALSHI_KEY_ID (UUID from dashboard).",
                    "KALSHI_API_KEY" if self.api_key else "KALSHI_KEY_ID",
                    "KALSHI_KEY_ID" if self.api_key else "KALSHI_API_KEY",
                )
            return {}

        ts = str(int(time.time() * 1000))
        try:
            sig = _build_rsa_signature(self.api_key, ts, method, path)
        except Exception as exc:
            LOGGER.warning("Kalshi: RSA signing failed (%s) — sending unauthenticated request.", exc)
            return {}

        return {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": sig,
            "KALSHI-ACCESS-TIMESTAMP": ts,
        }

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

        resolved_series = series_ticker or item.get("series_ticker", "") or ""
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
        """GET /markets with RSA auth; logs full response details on failure for debugging."""
        series_ticker = params.get("series_ticker", "")
        path = "/trade-api/v2/markets"
        try:
            response = self.http_client.session.get(
                f"{self.BASE_URL}/markets",
                params=params,
                headers=self._auth_headers(method="GET", path=path),
                timeout=self.http_client.timeout_seconds,
            )
            if response.status_code != 200:
                # Log response body (truncated) so operators can diagnose auth/endpoint issues.
                LOGGER.warning(
                    "Kalshi GET /markets returned HTTP %d (params=%s). "
                    "Response: %.300s",
                    response.status_code,
                    params,
                    response.text,
                )
                return []
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
          1. Series-specific fetches (authenticated, best coverage).
          2. Generic open-market fetch filtered by contract type pattern
             (unauthenticated fallback; gets whatever the public endpoint returns).
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
                "Kalshi: %d market(s) from series fetches: %s",
                len(all_markets),
                ", ".join(m["contract_id"] for m in all_markets),
            )
            return all_markets

        # Fallback 1: generic open-market fetch, filter by ticker pattern.
        LOGGER.warning(
            "Kalshi: series-specific fetches returned nothing — trying generic fetch. "
            "Tip: confirm KALSHI_API_KEY (PEM private key) and KALSHI_KEY_ID (dashboard UUID) are both set."
        )
        generic = self._get_markets({"status": "open", "limit": 200})
        if generic:
            # Log what we got so operators can diagnose wrong series tickers.
            sample = ", ".join(m["contract_id"] for m in generic[:10])
            LOGGER.info(
                "Kalshi generic fetch returned %d markets (first 10: %s). "
                "Filtering to cpi/unemployment types.",
                len(generic),
                sample,
            )
        relevant = [m for m in generic if m["contract_type"] in {"cpi", "unemployment"}]
        if relevant:
            LOGGER.info(
                "Kalshi generic fallback: %d relevant market(s) (%d CPI, %d unemployment)",
                len(relevant),
                sum(1 for m in relevant if m["contract_type"] == "cpi"),
                sum(1 for m in relevant if m["contract_type"] == "unemployment"),
            )
            return relevant

        # Fallback 2: hard-coded stubs (last resort — signals are flagged as synthetic).
        LOGGER.warning(
            "Kalshi: generic fetch also returned nothing relevant — falling back to hard-coded stubs. "
            "All signals will be flagged as synthetic. "
            "Check: correct base URL (%s), valid key_id+private_key, and series ticker names.",
            self.BASE_URL,
        )
        return self._fallback_stubs()

    @staticmethod
    def _fallback_stubs() -> list[dict[str, Any]]:
        # is_stub=True causes decision_reason to carry 'data_source=kalshi_stub',
        # which triggers the red warning banner in the weekly digest.
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
