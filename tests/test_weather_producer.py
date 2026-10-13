import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest
import requests
from confluent_kafka import KafkaException

from environmental_streaming.ingestion import weather_producer
from environmental_streaming.ingestion.weather_client import (
    OPEN_METEO_HOURLY_UNITS,
    HourlyWeatherResponse,
)
from environmental_streaming.ingestion.weather_locations import WeatherLocation

LOCATION = WeatherLocation("munich", "Munich", 48.13743, 11.57549, (2669,))
FETCHED = datetime(2026, 10, 9, 10, 15, tzinfo=timezone.utc)


def _response() -> HourlyWeatherResponse:
    return HourlyWeatherResponse(
        {
            "latitude": 48.14,
            "longitude": 11.58,
            "utc_offset_seconds": 0,
            "hourly_units": {"time": "iso8601", **OPEN_METEO_HOURLY_UNITS},
            "hourly": {
                "time": ["2026-10-09T10:00", "2026-10-09T11:00"],
                "temperature_2m": [None, 10.1],
                "relative_humidity_2m": [65, 60],
                "precipitation": [0.0, 0.1],
                "wind_speed_10m": [2.2, 3.0],
                "wind_direction_10m": [180, 190],
                "pressure_msl": [1010.1, 1011.2],
            },
        },
        FETCHED,
    )


def test_live_publish_connects_existing_conversion_and_confirmed_delivery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetch = Mock(return_value=_response())
    publish = Mock(return_value=6)
    monkeypatch.setattr(weather_producer, "fetch_hourly_weather", fetch)
    monkeypatch.setattr(weather_producer, "produce_weather_events", publish)
    report = weather_producer.publish_weather_location(LOCATION, 6, "test-broker:9092")
    fetch.assert_called_once_with(LOCATION, past_hours=6)
    events = publish.call_args.args[0]
    assert publish.call_args.kwargs == {"bootstrap_servers": "test-broker:9092"}
    assert len(events) == 6
    assert {event["measured_at_utc"] for event in events} == {
        "2026-10-09T10:00:00+00:00"
    }
    assert {event["fetched_at_utc"] for event in events} == {FETCHED.isoformat()}
    assert {event["unit"] for event in events} == {
        "degC",
        "%",
        "mm",
        "m/s",
        "degree",
        "hPa",
    }
    assert events[0]["value"] is None
    assert events[0]["latitude"] == LOCATION.latitude
    assert events[0]["grid_latitude"] == 48.14
    assert report == {
        "weather_location_id": "munich",
        "fetched_at_utc": FETCHED.isoformat(),
        "hour_count": 1,
        "topic": "environment.weather.canonical",
        "delivered_count": 6,
    }


def test_source_or_conversion_failures_never_publish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetch = Mock()
    publish = Mock()
    monkeypatch.setattr(weather_producer, "fetch_hourly_weather", fetch)
    monkeypatch.setattr(weather_producer, "produce_weather_events", publish)
    fetch.side_effect = requests.Timeout("source unavailable")
    with pytest.raises(requests.Timeout):
        weather_producer.publish_weather_location(LOCATION)
    fetch.side_effect = None
    for times in (
        ["2026-10-09T10:01", "2026-10-09T11:00"],
        ["2026-10-09T11:00", "2026-10-09T12:00"],
    ):
        response = _response()
        response.payload["hourly"]["time"] = times
        fetch.return_value = response
        with pytest.raises(ValueError):
            weather_producer.publish_weather_location(LOCATION)
    publish.assert_not_called()


def test_multi_location_isolates_operational_failures_and_counts_only_confirmed_batches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    locations = (
        LOCATION,
        WeatherLocation("stuttgart", "Stuttgart", 48.78, 9.18, (2936,)),
        WeatherLocation("hamburg", "Hamburg", 53.55, 9.99, (3071,)),
    )
    publish = Mock()
    monkeypatch.setattr(weather_producer, "publish_weather_location", publish)
    for failure in (
        requests.Timeout("source unavailable"),
        ValueError("bad response"),
        RuntimeError("delivery failed"),
        KafkaException("broker unavailable"),
        BufferError("queue full"),
    ):
        publish.reset_mock()
        publish.side_effect = [
            {"weather_location_id": "munich", "delivered_count": 24},
            failure,
            {"weather_location_id": "hamburg", "delivered_count": 18},
        ]
        report = weather_producer.publish_weather_locations(locations, 6, "test:9092")
        assert [call.args[0] for call in publish.call_args_list] == list(locations)
        assert all(
            call.kwargs == {"past_hours": 6, "bootstrap_servers": "test:9092"}
            for call in publish.call_args_list
        )
        assert report["locations_total"] == 3
        assert report["locations_succeeded"] == 2
        assert report["locations_failed"] == 1
        assert report["delivered_count"] == 42
        assert [item["weather_location_id"] for item in report["locations"]] == [
            "munich",
            "hamburg",
        ]
        assert report["errors"] == [
            {
                "weather_location_id": "stuttgart",
                "error_type": type(failure).__name__,
                "error": str(failure),
            }
        ]
    publish.side_effect = requests.Timeout("all unavailable")
    report = weather_producer.publish_weather_locations(locations)
    assert report["locations_failed"] == 3
    assert report["locations_succeeded"] == report["delivered_count"] == 0
    publish.reset_mock()
    with pytest.raises(ValueError, match="At least one"):
        weather_producer.publish_weather_locations(())
    publish.assert_not_called()
    publish.side_effect = TypeError("programming error")
    with pytest.raises(TypeError, match="programming error"):
        weather_producer.publish_weather_locations(locations)


def test_all_locations_command_reports_success_and_partial_failure_and_rejects_conflicting_selection(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    load = Mock(return_value=(LOCATION,))
    publish = Mock(
        return_value={"weather_location_id": "munich", "delivered_count": 24}
    )
    monkeypatch.setattr(weather_producer, "load_weather_locations", load)
    monkeypatch.setattr(weather_producer, "publish_weather_location", publish)
    monkeypatch.setattr(
        "sys.argv",
        [
            "weather_producer",
            "--all-locations",
            "--past-hours",
            "6",
            "--bootstrap-servers",
            "test:9092",
        ],
    )
    weather_producer.main()
    publish.assert_called_once_with(
        LOCATION, past_hours=6, bootstrap_servers="test:9092"
    )
    report = json.loads(capsys.readouterr().out)
    assert report["locations_succeeded"] == 1
    assert report["locations_failed"] == 0
    assert report["delivered_count"] == 24
    publish.side_effect = RuntimeError("partial delivery possible")
    with pytest.raises(SystemExit) as error:
        weather_producer.main()
    assert error.value.code == 1
    assert json.loads(capsys.readouterr().out)["locations_failed"] == 1
    load.reset_mock()
    publish.reset_mock()
    for arguments in ([], ["--all-locations", "--weather-location-id", "munich"]):
        monkeypatch.setattr("sys.argv", ["weather_producer", *arguments])
        with pytest.raises(SystemExit) as error:
            weather_producer.main()
        assert error.value.code == 2
    load.assert_not_called()
    publish.assert_not_called()


def test_live_command_loads_selected_location_and_reports_only_success(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    load = Mock(return_value=(LOCATION,))
    fetch = Mock(return_value=_response())
    publish = Mock(return_value=6)
    monkeypatch.setattr(weather_producer, "load_weather_locations", load)
    monkeypatch.setattr(weather_producer, "fetch_hourly_weather", fetch)
    monkeypatch.setattr(weather_producer, "produce_weather_events", publish)
    path = tmp_path / "locations.json"
    monkeypatch.setattr(
        "sys.argv",
        [
            "weather_producer",
            "--weather-location-id",
            "munich",
            "--locations-file",
            str(path),
            "--past-hours",
            "6",
            "--bootstrap-servers",
            "test:9092",
        ],
    )
    weather_producer.main()
    load.assert_called_once_with(path)
    fetch.assert_called_once_with(LOCATION, past_hours=6)
    assert publish.call_args.kwargs == {"bootstrap_servers": "test:9092"}
    assert json.loads(capsys.readouterr().out)["delivered_count"] == 6
    publish.side_effect = RuntimeError("delivery failed")
    with pytest.raises(RuntimeError, match="delivery failed"):
        weather_producer.main()
    assert capsys.readouterr().out == ""
    fetch.reset_mock()
    publish.reset_mock()
    monkeypatch.setattr(
        "sys.argv", ["weather_producer", "--weather-location-id", "unknown"]
    )
    with pytest.raises(SystemExit) as error:
        weather_producer.main()
    assert error.value.code == 2
    fetch.assert_not_called()
    publish.assert_not_called()
