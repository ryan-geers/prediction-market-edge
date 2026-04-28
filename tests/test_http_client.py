from unittest.mock import MagicMock

from src.core.http_client import HttpClient


def test_get_json_uses_session() -> None:
    client = HttpClient()
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = {"series": "X"}
    client.session.get = MagicMock(return_value=mock_resp)
    out = client.get_json("https://example.test/path", params={"a": "1"})
    assert out == {"series": "X"}
    client.session.get.assert_called_once()
