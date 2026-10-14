import json
from datetime import datetime, timedelta, timezone
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


def test_state_survives_restart_suppresses_receipt_only_changes_and_publishes_corrections(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = tmp_path / "data" / "state" / "weather.json"
    response = _response()
    fetch = Mock(return_value=response)
    publish = Mock(side_effect=lambda events, **_: len(events))
    monkeypatch.setattr(weather_producer, "fetch_hourly_weather", fetch)
    monkeypatch.setattr(weather_producer, "produce_weather_events", publish)
    first = weather_producer.publish_weather_location(LOCATION, state_path=path)
    assert first["delivered_count"] == 6 and first["skipped_count"] == 0
    original = path.read_text()
    response = HourlyWeatherResponse(response.payload, FETCHED + timedelta(minutes=1))
    fetch.return_value = response
    publish.reset_mock()
    second = weather_producer.publish_weather_location(LOCATION, state_path=path)
    assert second["delivered_count"] == 0 and second["skipped_count"] == 6
    assert second["fetched_at_utc"] == response.fetched_at_utc.isoformat()
    assert path.read_text() == original
    publish.assert_not_called()
    response.payload["hourly"]["temperature_2m"][0] = -5.0
    third = weather_producer.publish_weather_location(LOCATION, state_path=path)
    assert third["delivered_count"] == 1 and third["skipped_count"] == 5
    assert publish.call_args.args[0][0]["value"] == -5.0
    assert (
        publish.call_args.args[0][0]["fetched_at_utc"]
        == response.fetched_at_utc.isoformat()
    )
    response.payload["hourly"]["time"] = ["2026-10-09T11:00", "2026-10-09T12:00"]
    fetch.return_value = HourlyWeatherResponse(
        response.payload, FETCHED + timedelta(hours=3)
    )
    later = weather_producer.publish_weather_location(LOCATION, state_path=path)
    assert later["delivered_count"] == 12
    state = weather_producer.load_weather_state(path, "localhost:9092")
    keys = state["locations"]["munich"]
    assert len(keys) == 12
    assert all("T10:00" not in key for key in keys)


def test_delivery_and_atomic_write_failures_leave_existing_state_unchanged(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = tmp_path / "weather.json"
    response = _response()
    fetch = Mock(return_value=response)
    publish = Mock(side_effect=lambda events, **_: len(events))
    monkeypatch.setattr(weather_producer, "fetch_hourly_weather", fetch)
    monkeypatch.setattr(weather_producer, "produce_weather_events", publish)
    weather_producer.publish_weather_location(LOCATION, state_path=path)
    original = path.read_text()
    response.payload["hourly"]["wind_speed_10m"][0] = 4.0
    publish.side_effect = RuntimeError("partial delivery")
    with pytest.raises(RuntimeError, match="partial delivery"):
        weather_producer.publish_weather_location(LOCATION, state_path=path)
    assert path.read_text() == original
    missing_path = tmp_path / "never-confirmed.json"
    with pytest.raises(RuntimeError):
        weather_producer.publish_weather_location(LOCATION, state_path=missing_path)
    assert not missing_path.exists()
    publish.side_effect = None
    publish.return_value = 0
    with pytest.raises(RuntimeError, match="delivery count"):
        weather_producer.publish_weather_location(LOCATION, state_path=path)
    assert path.read_text() == original
    publish.return_value = 1
    monkeypatch.setattr(
        weather_producer.os, "replace", Mock(side_effect=OSError("disk failure"))
    )
    with pytest.raises(OSError, match="disk failure"):
        weather_producer.publish_weather_location(LOCATION, state_path=path)
    assert path.read_text() == original
    assert not list(tmp_path.glob("*.tmp"))


def test_invalid_or_wrong_destination_state_fails_closed_before_fetch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = tmp_path / "weather.json"
    good = weather_producer.load_weather_state(path, "localhost:9092")
    bad_states = [
        None,
        [],
        {},
        {**good, "version": True},
        {**good, "version": 2},
        {**good, "topic": "other"},
        {**good, "bootstrap_servers": "other:9092"},
        {**good, "locations": []},
        {**good, "locations": {"munich": []}},
        {**good, "locations": {"munich": {"invalid key": "0" * 64}}},
        {**good, "locations": {"munich": {"[]": "bad fingerprint"}}},
    ]
    fetch = Mock()
    monkeypatch.setattr(weather_producer, "fetch_hourly_weather", fetch)
    for raw in ["invalid JSON", *(json.dumps(state) for state in bad_states)]:
        path.write_text(raw)
        with pytest.raises(ValueError):
            weather_producer.publish_weather_locations((LOCATION,), state_path=path)
        assert path.read_text() == raw
    fetch.assert_not_called()


def test_stateful_multi_location_cli_commits_successes_independently(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    locations = (
        LOCATION,
        WeatherLocation("stuttgart", "Stuttgart", 48.78, 9.18, (2936,)),
        WeatherLocation("hamburg", "Hamburg", 53.55, 9.99, (3071,)),
    )
    path = tmp_path / "weather.json"
    monkeypatch.setattr(weather_producer, "load_weather_locations", lambda _: locations)
    fetch = Mock(
        side_effect=[_response(), requests.Timeout("unavailable"), _response()]
    )
    publish = Mock(side_effect=lambda events, **_: len(events))
    monkeypatch.setattr(weather_producer, "fetch_hourly_weather", fetch)
    monkeypatch.setattr(weather_producer, "produce_weather_events", publish)
    monkeypatch.setattr(
        "sys.argv", ["weather_producer", "--all-locations", "--state-path", str(path)]
    )
    with pytest.raises(SystemExit) as error:
        weather_producer.main()
    assert error.value.code == 1
    report = json.loads(capsys.readouterr().out)
    assert report["delivered_count"] == 12
    assert report["locations_failed"] == 1
    assert set(
        weather_producer.load_weather_state(path, "localhost:9092")["locations"]
    ) == {"munich", "hamburg"}
    fetch.side_effect = None
    fetch.return_value = _response()
    weather_producer.main()
    report = json.loads(capsys.readouterr().out)
    assert report["locations_failed"] == 0
    assert report["delivered_count"] == 6 and report["skipped_count"] == 12
    assert set(
        weather_producer.load_weather_state(path, "localhost:9092")["locations"]
    ) == {"munich", "stuttgart", "hamburg"}


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
