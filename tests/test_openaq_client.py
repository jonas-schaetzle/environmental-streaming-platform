from typing import Any

import pytest

from src import openaq_client


class FakeResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self.payload


def test_get_required_env_var_returns_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAQ_API_KEY", "test-api-key")

    assert openaq_client.get_required_env_var("OPENAQ_API_KEY") == "test-api-key"


def test_get_required_env_var_rejects_missing_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OPENAQ_API_KEY", raising=False)

    with pytest.raises(ValueError, match="OPENAQ_API_KEY"):
        openaq_client.get_required_env_var("OPENAQ_API_KEY")


def test_fetch_json_sends_api_key_header(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    def fake_get(
        url: str,
        headers: dict[str, str],
        timeout: int,
    ) -> FakeResponse:
        calls.append({"url": url, "headers": headers, "timeout": timeout})
        return FakeResponse({"results": [{"id": 2178}]})

    monkeypatch.setattr(openaq_client.requests, "get", fake_get)

    result = openaq_client.fetch_json(
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


def test_extract_sensor_ids_returns_sorted_unique_ids() -> None:
    payload = {
        "results": [
            {"sensorsId": 3918},
            {"sensorsId": 3916},
            {"sensorsId": 3918},
            {"sensorsId": None},
            {},
        ]
    }

    assert openaq_client.extract_sensor_ids(payload) == [3916, 3918]
