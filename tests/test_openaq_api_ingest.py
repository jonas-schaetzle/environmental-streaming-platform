import sys
from typing import Any

import pytest

from src import openaq_api_ingest


def test_get_required_env_var_returns_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAQ_API_KEY", "test-api-key")

    result = openaq_api_ingest.get_required_env_var("OPENAQ_API_KEY")

    assert result == "test-api-key"


def test_get_required_env_var_rejects_missing_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OPENAQ_API_KEY", raising=False)

    with pytest.raises(ValueError, match="OPENAQ_API_KEY"):
        openaq_api_ingest.get_required_env_var("OPENAQ_API_KEY")


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

    result = openaq_api_ingest.extract_sensor_ids(payload)

    assert result == [3916, 3918]


def test_ingest_location_latest_writes_latest_and_sensor_payloads(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pytest.TempPathFactory,
) -> None:
    latest_output_path = tmp_path / "openaq_location_latest_raw.json"
    sensor_output_dir = tmp_path / "openaq_sensors"

    monkeypatch.setattr(openaq_api_ingest, "LATEST_OUTPUT_PATH", latest_output_path)
    monkeypatch.setattr(openaq_api_ingest, "SENSOR_OUTPUT_DIR", sensor_output_dir)

    fetched_urls: list[str] = []

    def fake_fetch_json(url: str, api_key: str) -> dict[str, Any]:
        fetched_urls.append(url)
        assert api_key == "test-api-key"

        if url.endswith("/locations/2178/latest?limit=100"):
            return {"results": [{"sensorsId": 3916}, {"sensorsId": 3918}]}

        if url.endswith("/sensors/3916"):
            return {"results": [{"id": 3916}]}

        if url.endswith("/sensors/3918"):
            return {"results": [{"id": 3918}]}

        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(openaq_api_ingest, "fetch_json", fake_fetch_json)

    openaq_api_ingest.ingest_location_latest(2178, "test-api-key")

    assert latest_output_path.exists()
    assert (sensor_output_dir / "sensor_3916.json").exists()
    assert (sensor_output_dir / "sensor_3918.json").exists()
    assert fetched_urls == [
        "https://api.openaq.org/v3/locations/2178/latest?limit=100",
        "https://api.openaq.org/v3/sensors/3916",
        "https://api.openaq.org/v3/sensors/3918",
    ]


def test_parse_args_accepts_location_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        ["openaq_api_ingest.py", "--location-id", "1234"],
    )

    args = openaq_api_ingest.parse_args()

    assert args.location_id == 1234
