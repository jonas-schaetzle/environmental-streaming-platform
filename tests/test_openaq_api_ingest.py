from typing import Any

import pytest

from src import openaq_api_ingest


class FakeResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self.payload


def test_fetch_json_sends_api_key_header(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    def fake_get(
        url: str,
        headers: dict[str, str],
        timeout: int,
    ) -> FakeResponse:
        calls.append(
            {
                "url": url,
                "headers": headers,
                "timeout": timeout,
            }
        )
        return FakeResponse({"results": [{"id": 2178}]})

    monkeypatch.setattr(openaq_api_ingest.requests, "get", fake_get)

    result = openaq_api_ingest.fetch_json(
        "https://api.openaq.org/v3/locations/2178/latest",
        api_key="test-api-key",
        timeout_seconds=10,
    )

    assert result == {"results": [{"id": 2178}]}
    assert calls == [
        {
            "url": "https://api.openaq.org/v3/locations/2178/latest",
            "headers": {"X-API-Key": "test-api-key"},
            "timeout": 10,
        }
    ]


def test_fetch_json_rejects_empty_api_key() -> None:
    with pytest.raises(ValueError, match="API key"):
        openaq_api_ingest.fetch_json(
            "https://api.openaq.org/v3/locations/2178/latest",
            api_key="",
        )
