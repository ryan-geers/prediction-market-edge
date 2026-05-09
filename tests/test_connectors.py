import json
from pathlib import Path

import pytest

from src.connectors.bea import BeaConnector
from src.connectors.bls import BlsConnector
from src.connectors.fred import FredConnector
from src.connectors.kalshi import KalshiConnector
from src.connectors.polymarket import PolymarketConnector

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "connectors"


def _load_json(name: str):
    return json.loads((FIXTURES_DIR / name).read_text())


def test_fred_fallback_data():
    connector = FredConnector()
    connector._fetch_series = lambda _series: None  # type: ignore[method-assign]
    rows = connector.fetch()
    assert any(row["series"] == "PPIACO" for row in rows)


def test_bls_fallback_data():
    connector = BlsConnector()
    connector.http_client.session.post = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("fail"))  # type: ignore[method-assign]
    rows = connector.fetch()
    assert rows[0]["series"] == "LNS14000000"


def test_bea_fallback_without_api_key():
    connector = BeaConnector(api_key=None)
    rows = connector.fetch()
    assert rows[0]["series"] == "PCEPI"


def test_kalshi_normalization():
    connector = KalshiConnector()
    row = connector._normalize_market({"ticker": "KXU3-26MAY-T4.8", "title": "X", "yes_bid": 40, "yes_ask": 45, "last_price": 43}, series_ticker="KXU3")
    assert row["best_bid"] == 0.4
    assert row["best_ask"] == 0.45
    assert row["contract_type"] == "unemployment"
    assert row["threshold"] == 4.8
    assert row["series_ticker"] == "KXU3"


def test_kalshi_normalization_cpi():
    connector = KalshiConnector()
    row = connector._normalize_market({"ticker": "CPI-MAY-OVER-0.3", "title": "CPI", "yes_bid": 44, "yes_ask": 48}, series_ticker="KXCPI")
    assert row["contract_type"] == "cpi"
    # CPI tickers use OVER-0.3 format, not -T{value}; threshold is None and handled statically by the module.
    assert row["threshold"] is None


def test_kalshi_fallback_stubs_have_both_types():
    stubs = KalshiConnector._fallback_stubs()
    types = {s["contract_type"] for s in stubs}
    assert "cpi" in types
    assert "unemployment" in types


def test_fred_parse_fixture():
    value = FredConnector.parse_latest_value(_load_json("fred_observations.json"))
    assert value == 246.7


def test_bls_parse_fixture():
    rows = BlsConnector.parse_response(_load_json("bls_response.json"))
    assert rows[0]["series"] == "LNS14000000"
    assert rows[0]["value"] == 4.1


def test_bls_parse_history_fixture():
    rows = BlsConnector.parse_history_response(_load_json("bls_response.json"))
    assert len(rows) == 1
    assert rows[0]["date"] == "2026-03-01"


def test_bls_parse_history_skips_dash_values():
    data = {
        "Results": {
            "series": [
                {
                    "seriesID": "LNS14000000",
                    "data": [
                        {"year": "2025", "period": "M01", "value": "-"},
                        {"year": "2025", "period": "M02", "value": "4.1"},
                    ],
                }
            ]
        }
    }
    rows = BlsConnector.parse_history_response(data)
    assert len(rows) == 1
    assert rows[0]["date"] == "2025-02-01"
    assert rows[0]["value"] == 4.1


def test_bea_parse_fixture():
    rows = BeaConnector.parse_response(_load_json("bea_response.json"))
    assert rows[0]["series"] == "PCEPI"
    assert rows[0]["value"] == 123.8


def test_bea_parse_history_fixture():
    rows = BeaConnector.parse_history_response(_load_json("bea_response.json"))
    assert len(rows) == 2
    assert rows[-1]["date"] == "2026-03-01"


def test_kalshi_parse_markets_fixture():
    connector = KalshiConnector()
    rows = connector.parse_markets(_load_json("kalshi_markets.json"))
    assert len(rows) == 1
    assert rows[0]["contract_id"] == "CPI-MAY-OVER-03"


def test_polymarket_parse_markets_fixture():
    connector = PolymarketConnector()
    rows = connector.parse_markets(_load_json("polymarket_markets.json"))
    assert len(rows) == 1
    assert rows[0]["contract_id"] == "pm-abc"
