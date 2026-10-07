import copy
import json
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from environmental_streaming.ingestion import weather_events
from environmental_streaming.ingestion.weather_client import (
    OPEN_METEO_HOURLY_UNITS,
    HourlyWeatherResponse,
)
from environmental_streaming.ingestion.weather_locations import WeatherLocation
from environmental_streaming.processing.weather_model import (
    WEATHER_PARAMETER_UNITS,
    canonical_weather_schema,
    parse_weather_observations,
)

LOCATION = WeatherLocation("munich", "Munich", 48.13743, 11.57549, (2669,))
FETCHED = datetime(2026, 10, 6, 10, 15, 0, 123456, tzinfo=timezone.utc)


def _response() -> HourlyWeatherResponse:
    return HourlyWeatherResponse(
        {
            "latitude": 48.14,
            "longitude": 11.58,
            "utc_offset_seconds": 0,
            "hourly_units": {"time": "iso8601", **OPEN_METEO_HOURLY_UNITS},
            "hourly": {
                "time": ["2026-10-06T09:00", "2026-10-06T10:00"],
                "temperature_2m": [-5.0, 10.1],
                "relative_humidity_2m": [65, 60],
                "precipitation": [0.0, 0.1],
                "wind_speed_10m": [2.2, 3.0],
                "wind_direction_10m": [180, 190],
                "pressure_msl": [1010.1, 1011.2],
            },
        },
        FETCHED,
    )


def test_conversion_matches_contract_and_preserves_source_lineage() -> None:
    response = _response()
    original = copy.deepcopy(response)
    events = weather_events.build_canonical_weather_events(LOCATION, response)
    assert weather_events.CANONICAL_HOURLY_UNITS == WEATHER_PARAMETER_UNITS
    assert len(events) == 12
    for index, event in enumerate(events):
        assert set(event) == set(canonical_weather_schema().fieldNames())
        assert event["source"] == "open_meteo"
        assert event["source_dataset"] == "forecast_best_match"
        assert event["weather_location_id"] == "munich"
        assert event["city"] == "Munich"
        assert (event["latitude"], event["longitude"]) == (48.13743, 11.57549)
        assert (event["grid_latitude"], event["grid_longitude"]) == (48.14, 11.58)
        assert event["unit"] == WEATHER_PARAMETER_UNITS[event["parameter"]]
        assert (
            event["value"] == response.payload["hourly"][event["parameter"]][index // 6]
        )
        assert event["measured_at_utc"] == (
            "2026-10-06T09:00:00+00:00" if index < 6 else "2026-10-06T10:00:00+00:00"
        )
        assert event["fetched_at_utc"] == FETCHED.isoformat()
    assert response == original
    assert weather_events.build_canonical_weather_events(LOCATION, response) == events


def test_conversion_excludes_future_hours_relative_to_receipt_not_wall_clock() -> None:
    payload = _response().payload
    for fetched in (
        datetime(2026, 10, 6, 9, tzinfo=timezone.utc),
        datetime(2026, 10, 6, 9, 59, tzinfo=timezone.utc),
    ):
        events = weather_events.build_canonical_weather_events(
            LOCATION, HourlyWeatherResponse(payload, fetched)
        )
        assert len(events) == 6
        assert all(
            event["measured_at_utc"] == "2026-10-06T09:00:00+00:00" for event in events
        )
    assert (
        weather_events.build_canonical_weather_events(
            LOCATION,
            HourlyWeatherResponse(
                payload, datetime(2026, 10, 6, 8, tzinfo=timezone.utc)
            ),
        )
        == []
    )


def test_conversion_rejects_ambiguous_times_duplicates_and_broken_envelopes() -> None:
    for label in (
        "2026-02-30T09:00",
        "2026-10-06T09:01",
        "2026-10-06T09:00:00",
        "2026-10-06T09:00+02:00",
        "2026-10-06",
        "invalid",
        "2026-10-06T10:00",
    ):
        response = _response()
        response.payload["hourly"]["time"][0] = label
        with pytest.raises(ValueError):
            weather_events.build_canonical_weather_events(LOCATION, response)
    for fetched in (
        FETCHED.replace(tzinfo=None),
        FETCHED.astimezone(timezone(timedelta(hours=2))),
    ):
        with pytest.raises(ValueError, match="explicit UTC offset"):
            weather_events.build_canonical_weather_events(
                LOCATION, HourlyWeatherResponse(_response().payload, fetched)
            )
    for field, value in (
        ("utc_offset_seconds", 7200),
        ("hourly", {}),
        ("hourly_units", {}),
    ):
        response = _response()
        response.payload[field] = value
        with pytest.raises(ValueError):
            weather_events.build_canonical_weather_events(LOCATION, response)
    response = _response()
    response.payload["hourly"]["temperature_2m"].pop()
    with pytest.raises(ValueError, match="align"):
        weather_events.build_canonical_weather_events(LOCATION, response)


def test_converted_events_route_through_weather_model_without_hiding_bad_values(
    spark: SparkSession,
) -> None:
    response = _response()
    response.payload["hourly"]["temperature_2m"][1] = None
    response.payload["hourly"]["wind_speed_10m"][1] = -1.0
    events = weather_events.build_canonical_weather_events(LOCATION, response)
    assert events[6]["value"] is None
    assert events[9]["value"] == -1.0
    frame = spark.createDataFrame(
        [(json.dumps(event),) for event in events], ["raw_value"]
    )
    rows = parse_weather_observations(frame).collect()
    valid = [row for row in rows if not row.validation_error]
    assert len(valid) == 10
    assert any(row.value == -5.0 for row in valid)
    assert {
        row.parameter: row.validation_error for row in rows if row.validation_error
    } == {
        "temperature_2m": "missing_value",
        "wind_speed_10m": "value_out_of_range",
    }


def test_export_uses_snapshot_metadata_and_never_overwrites_files(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    response = _response()
    capture = {
        "source": "open_meteo",
        "source_dataset": "forecast_best_match",
        "location": asdict(LOCATION),
        "fetched_at_utc": FETCHED.isoformat(),
        "response": response.payload,
    }
    path = tmp_path / "capture.json"
    path.write_text(json.dumps(capture), encoding="utf-8")
    original_capture = path.read_text(encoding="utf-8")
    directory = tmp_path / "data" / "weather"
    monkeypatch.setattr(weather_events, "WEATHER_CAPTURE_DIRECTORY", directory)
    monkeypatch.setattr("sys.argv", ["weather_events", "--capture-file", str(path)])
    weather_events.main()
    report = json.loads(capsys.readouterr().out)
    output = Path(report["output_path"])
    original = output.read_text(encoding="utf-8")
    assert output.parent == directory
    assert report["event_count"] == 12
    assert [json.loads(line) for line in original.splitlines()] == (
        weather_events.build_canonical_weather_events(LOCATION, response)
    )
    assert path.read_text(encoding="utf-8") == original_capture
    with pytest.raises(FileExistsError):
        weather_events.main()
    assert output.read_text(encoding="utf-8") == original
    for field, value in (("source", "openaq"), ("source_dataset", "era5")):
        path.write_text(json.dumps({**capture, field: value}), encoding="utf-8")
        with pytest.raises(ValueError, match="Capture must use"):
            weather_events.main()
