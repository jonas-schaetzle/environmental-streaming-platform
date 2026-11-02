import copy
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest
import requests

from environmental_streaming.ingestion import weather_client
from environmental_streaming.ingestion.weather_locations import WeatherLocation
from environmental_streaming.processing.weather_model import WEATHER_PARAMETER_UNITS

LOCATION = WeatherLocation("munich", "Munich", 48.13743, 11.57549, (2669,))


def _payload() -> dict:
    return {
        "latitude": 48.14,
        "longitude": 11.58,
        "utc_offset_seconds": 0,
        "hourly_units": {"time": "iso8601", **weather_client.OPEN_METEO_HOURLY_UNITS},
        "hourly": {
            "time": ["2026-10-06T09:00", "2026-10-06T10:00"],
            "temperature_2m": [10.1, None],
            "relative_humidity_2m": [65, 60],
            "precipitation": [0.0, 0.1],
            "wind_speed_10m": [2.2, 3.0],
            "wind_direction_10m": [180, 190],
            "pressure_msl": [1010.1, 1011.2],
        },
    }


def _response(payload: object) -> Mock:
    return Mock(spec=requests.Response, json=Mock(return_value=payload))


def _http_error(status: int) -> requests.HTTPError:
    return requests.HTTPError(f"HTTP {status}", response=Mock(status_code=status))


def test_fetch_requests_contract_parameters_and_preserves_raw_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _payload()
    response = _response(payload)
    get = Mock(return_value=response)
    receipt_time = datetime(2026, 10, 6, 10, 15, tzinfo=timezone.utc)
    clock = Mock()
    clock.now.return_value = receipt_time
    monkeypatch.setattr(weather_client.requests, "get", get)
    monkeypatch.setattr(weather_client, "datetime", clock)

    def decode() -> dict:
        clock.now.assert_called_once_with(timezone.utc)
        return payload

    response.json.side_effect = decode
    result = weather_client.fetch_hourly_weather(
        LOCATION, past_hours=5, timeout_seconds=10
    )
    assert result.payload is payload
    assert result.fetched_at_utc == receipt_time
    assert result.payload["hourly"]["temperature_2m"][1] is None
    assert set(weather_client.OPEN_METEO_HOURLY_UNITS) == set(WEATHER_PARAMETER_UNITS)
    get.assert_called_once_with(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": 48.13743,
            "longitude": 11.57549,
            "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,wind_direction_10m,pressure_msl",
            "timezone": "UTC",
            "timeformat": "iso8601",
            "temperature_unit": "celsius",
            "wind_speed_unit": "ms",
            "precipitation_unit": "mm",
            "past_hours": 5,
            "forecast_hours": 1,
        },
        timeout=10,
    )


def test_fetch_retries_transient_failures_with_bounded_exponential_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for failure in (
        requests.Timeout("timeout"),
        requests.ConnectionError("connection"),
        *(_http_error(status) for status in (429, 500, 502, 503, 504)),
    ):
        get = Mock(side_effect=[failure, failure, _response(_payload())])
        sleep = Mock()
        monkeypatch.setattr(weather_client.requests, "get", get)
        monkeypatch.setattr(weather_client.time, "sleep", sleep)
        assert (
            weather_client.fetch_hourly_weather(
                LOCATION, max_attempts=3, backoff_seconds=0.5
            ).payload
            == _payload()
        )
        assert get.call_count == 3
        assert [call.args[0] for call in sleep.call_args_list] == [0.5, 1.0]

    failure = requests.Timeout("exhausted")
    get = Mock(side_effect=failure)
    sleep = Mock()
    monkeypatch.setattr(weather_client.requests, "get", get)
    monkeypatch.setattr(weather_client.time, "sleep", sleep)
    with pytest.raises(requests.Timeout, match="exhausted"):
        weather_client.fetch_hourly_weather(LOCATION, max_attempts=2)
    assert get.call_count == 2
    assert sleep.call_count == 1


def test_fetch_rejects_permanent_errors_and_bad_responses_without_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for status in (400, 401, 403, 404):
        get = Mock(side_effect=_http_error(status))
        sleep = Mock()
        monkeypatch.setattr(weather_client.requests, "get", get)
        monkeypatch.setattr(weather_client.time, "sleep", sleep)
        with pytest.raises(requests.HTTPError):
            weather_client.fetch_hourly_weather(LOCATION)
        get.assert_called_once()
        sleep.assert_not_called()

    invalid_payloads: list[object] = [
        None,
        [],
        {},
        {**_payload(), "utc_offset_seconds": 7200},
    ]
    for field in ("hourly", "hourly_units"):
        invalid_payloads.append({**_payload(), field: []})
    for times in ([], None, [123], [" "]):
        payload = _payload()
        payload["hourly"]["time"] = times
        invalid_payloads.append(payload)
    for parameter in weather_client.OPEN_METEO_HOURLY_UNITS:
        for invalid_value in (None, [1]):
            payload = _payload()
            payload["hourly"][parameter] = invalid_value
            invalid_payloads.append(payload)
        payload = _payload()
        payload["hourly_units"][parameter] = "wrong unit"
        invalid_payloads.append(payload)
    payload = _payload()
    payload["hourly_units"]["time"] = "unixtime"
    invalid_payloads.append(payload)
    for payload in invalid_payloads:
        get = Mock(return_value=_response(payload))
        monkeypatch.setattr(weather_client.requests, "get", get)
        with pytest.raises(ValueError):
            weather_client.fetch_hourly_weather(LOCATION)
        get.assert_called_once()

    response = _response(None)
    response.json.side_effect = requests.exceptions.JSONDecodeError("invalid", "x", 0)
    get = Mock(return_value=response)
    monkeypatch.setattr(weather_client.requests, "get", get)
    with pytest.raises(requests.exceptions.JSONDecodeError):
        weather_client.fetch_hourly_weather(LOCATION)
    get.assert_called_once()


def test_fetch_rejects_invalid_request_settings_before_http(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    get = Mock()
    monkeypatch.setattr(weather_client.requests, "get", get)
    for field, values in (
        ("past_hours", (0, -1, True, 1.5)),
        ("max_attempts", (0, -1, True, 1.5)),
        ("timeout_seconds", (0, -1, float("nan"), float("inf"))),
        ("backoff_seconds", (-1, float("nan"), float("inf"))),
    ):
        for value in values:
            with pytest.raises(ValueError, match=field):
                weather_client.fetch_hourly_weather(LOCATION, **{field: value})
    get.assert_not_called()


def test_capture_command_saves_response_and_lineage_and_rejects_unknown_location(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    receipt_time = datetime(2026, 10, 6, 10, 15, tzinfo=timezone.utc)
    payload = _payload()
    fetch = Mock(
        return_value=weather_client.HourlyWeatherResponse(payload, receipt_time)
    )
    monkeypatch.setattr(weather_client, "fetch_hourly_weather", fetch)
    monkeypatch.setattr(weather_client, "load_weather_locations", lambda _: (LOCATION,))
    directory = tmp_path / "data" / "weather"
    monkeypatch.setattr(weather_client, "WEATHER_CAPTURE_DIRECTORY", directory)
    monkeypatch.setattr(
        "sys.argv", ["weather_client", "--weather-location-id", "munich"]
    )
    weather_client.main()
    report = json.loads(capsys.readouterr().out)
    saved = json.loads(Path(report["output_path"]).read_text(encoding="utf-8"))
    assert saved["response"] == payload
    assert saved["fetched_at_utc"] == receipt_time.isoformat()
    assert saved["source"] == "open_meteo"
    assert saved["source_dataset"] == "forecast_best_match"
    assert saved["location"]["latitude"] == LOCATION.latitude
    assert saved["location"]["latitude"] != payload["latitude"]
    assert saved["location"]["openaq_location_ids"] == [2669]
    assert report["hour_count"] == 2
    fetch.assert_called_once_with(LOCATION, past_hours=3)
    original = copy.deepcopy(saved)
    with pytest.raises(FileExistsError):
        weather_client.main()
    assert (
        json.loads(Path(report["output_path"]).read_text(encoding="utf-8")) == original
    )

    fetch.reset_mock()
    monkeypatch.setattr(
        "sys.argv", ["weather_client", "--weather-location-id", "unknown"]
    )
    with pytest.raises(SystemExit) as error:
        weather_client.main()
    assert error.value.code == 2
    fetch.assert_not_called()
