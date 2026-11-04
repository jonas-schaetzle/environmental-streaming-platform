from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, Mock

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, LongType, StringType

from environmental_streaming.ingestion.openaq_producer import OpenAQLocation
from environmental_streaming.ingestion.weather_locations import WeatherLocation
from environmental_streaming.lakehouse.tables import (
    ICEBERG_HOURLY_AGGREGATE_TABLE,
    ICEBERG_WEATHER_CANONICAL_TABLE,
)
from environmental_streaming.processing.weather_model import (
    WEATHER_PARAMETER_UNITS,
    canonical_weather_schema,
)
from environmental_streaming.products import hourly_environmental_insights as insights


START = datetime(2026, 11, 3, 10, tzinfo=timezone.utc)
ACTIVE = (
    OpenAQLocation(2669, "Munich Stachus"),
    OpenAQLocation(2936, "Stuttgart Bad Cannstatt"),
    OpenAQLocation(3015, "Hamburg Sternschanze"),
)
WEATHER_LOCATIONS = (
    WeatherLocation("munich", "Munich", 48.13743, 11.57549, (2669,)),
    WeatherLocation("stuttgart", "Stuttgart", 48.78232, 9.17702, (2936,)),
    WeatherLocation("hamburg", "Hamburg", 53.55073, 9.99302, (3071, 3015)),
)
AIR_SCHEMA = """
    source string, location_id long, parameter string, unit string,
    window_start timestamp, window_end timestamp, measurement_count long,
    average_value double, minimum_value double, maximum_value double,
    latest_measured_at_utc timestamp
"""


def _air(location_id=2669, hour=0, **overrides):
    start = START + timedelta(hours=hour)
    return {
        "source": "openaq",
        "location_id": location_id,
        "parameter": "pm25",
        "unit": insights.AIR_UNIT,
        "window_start": start,
        "window_end": start + timedelta(hours=1),
        "measurement_count": 2,
        "average_value": 12.0,
        "minimum_value": 10.0,
        "maximum_value": 14.0,
        "latest_measured_at_utc": start + timedelta(minutes=45),
        **overrides,
    }


def _weather(parameter="temperature_2m", hour=0, city="munich", **overrides):
    location = next(w for w in WEATHER_LOCATIONS if w.weather_location_id == city)
    return {
        "source": "open_meteo",
        "source_dataset": "forecast_best_match",
        "weather_location_id": city,
        "city": location.city,
        "latitude": location.latitude,
        "longitude": location.longitude,
        "grid_latitude": location.latitude + 0.01,
        "grid_longitude": location.longitude + 0.01,
        "parameter": parameter,
        "value": 15.0,
        "unit": WEATHER_PARAMETER_UNITS[parameter],
        "measured_at_utc": START + timedelta(hours=hour),
        "fetched_at_utc": START + timedelta(hours=3),
        "kafka_topic": "environment.weather.canonical",
        "kafka_partition": 0,
        "kafka_offset": 1,
        **overrides,
    }


def _install_inputs(spark, monkeypatch, air, weather):
    schema = (
        canonical_weather_schema()
        .add("kafka_topic", StringType())
        .add("kafka_partition", IntegerType())
        .add("kafka_offset", LongType())
    )
    frames = {
        ICEBERG_HOURLY_AGGREGATE_TABLE: spark.createDataFrame(air, AIR_SCHEMA),
        ICEBERG_WEATHER_CANONICAL_TABLE: spark.createDataFrame(weather, schema),
    }
    monkeypatch.setattr(
        insights,
        "_read_snapshot",
        lambda _spark, table, _snapshot: (
            frames[table],
            11 if table == ICEBERG_HOURLY_AGGREGATE_TABLE else 22,
        ),
    )
    return frames


def _report(spark, locations=ACTIVE, **overrides):
    return insights.collect_hourly_environmental_report(
        spark,
        locations,
        WEATHER_LOCATIONS,
        **{
            "hours": 2,
            "end_at": START + timedelta(hours=2),
            "now": START + timedelta(hours=3),
            **overrides,
        },
    )


def test_hour_alignment_keeps_missing_values_and_station_histories(
    spark: SparkSession, process_timezone: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    session = spark.newSession()
    session.conf.set("spark.sql.session.timeZone", "America/New_York")
    air = [
        _air(),
        _air(hour=1),
        _air(2936, hour=1),
        _air(3071),
        _air(2936, unit="ppm"),
        _air(source="other"),
        _air(parameter="no2"),
    ]
    weather = [
        _weather(
            parameter,
            hour=1 if parameter == "precipitation" else 0,
            value=0.0 if parameter == "precipitation" else 15.0,
        )
        for parameter in WEATHER_PARAMETER_UNITS
    ] + [
        _weather("precipitation", hour=0, value=99.0),
        _weather("precipitation", hour=2, value=2.5),
        _weather(hour=1, value=20.0),
        _weather(hour=2, value=99.0),
        _weather(city="stuttgart", hour=1),
        _weather(city="hamburg"),
        _weather(source_dataset="different_dataset", value=999.0),
    ]
    _install_inputs(session, monkeypatch, air, weather)
    report = _report(session)
    munich, stuttgart, hamburg = report["locations"]
    first, second = munich["hours"]
    assert first["window_start_utc"] == "2026-11-03T10:00:00Z", process_timezone
    assert first["window_end_utc"] == "2026-11-03T11:00:00Z"
    assert first["air_quality"]["measurement_count"] == 2
    assert first["air_quality"]["latest_measured_at_utc"] == "2026-11-03T10:45:00Z"
    assert first["weather_status"] == "complete"
    assert first["weather"]["precipitation"]["value"] == 0.0
    assert first["weather"]["precipitation"]["valid_at_utc"] == "2026-11-03T11:00:00Z"
    assert first["weather"]["temperature_2m"]["valid_at_utc"] == "2026-11-03T10:00:00Z"
    assert second["weather_status"] == "partial"
    assert second["weather"]["temperature_2m"]["value"] == 20.0
    assert second["weather"]["precipitation"]["value"] == 2.5
    assert second["weather"]["pressure_msl"] is None
    assert stuttgart["hours"][0]["air_quality_status"] == "missing_finalized_aggregate"
    assert stuttgart["hours"][0]["weather_status"] == "missing"
    assert stuttgart["hours"][1]["air_quality"] is not None
    assert all(point["air_quality"] is None for point in hamburg["hours"])
    assert hamburg["location_id"] == 3015
    assert len(hamburg["hours"]) == 2
    assert munich["coverage"] == {
        "expected_hours": 2,
        "air_quality_hours": 2,
        "complete_weather_hours": 1,
        "complete_join_hours": 1,
    }
    assert report["inputs"]["weather"]["snapshot_id"] == 22


def test_refresh_uses_accepted_correction_without_changing_air_statistics(
    spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install_inputs(spark, monkeypatch, [_air()], [_weather()])
    before = _report(
        spark,
        locations=ACTIVE[:1],
        hours=1,
        end_at=START + timedelta(hours=1),
        now=START + timedelta(days=1, hours=3),
    )
    newer = _weather(
        value=19.0, kafka_offset=200, fetched_at_utc=START + timedelta(days=1)
    )
    _install_inputs(spark, monkeypatch, [_air()], [newer])
    after = _report(
        spark,
        locations=ACTIVE[:1],
        hours=1,
        end_at=START + timedelta(hours=1),
        now=START + timedelta(days=1, hours=3),
    )
    old_hour, new_hour = (
        before["locations"][0]["hours"][0],
        after["locations"][0]["hours"][0],
    )
    assert old_hour["air_quality"] == new_hour["air_quality"]
    assert old_hour["weather"]["temperature_2m"]["value"] == 15.0
    accepted = new_hour["weather"]["temperature_2m"]
    assert accepted["value"] == 19.0
    assert accepted["fetched_at_utc"] == "2026-11-04T10:00:00Z"
    assert accepted["kafka"]["offset"] == 200


def test_duplicate_trusted_identities_fail_instead_of_multiplying_rows(
    spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    for air, weather, reason in (
        ([_air(), _air()], [], "Hourly aggregates are not unique"),
        ([_air()], [_weather(), _weather()], "duplicate parameter/hour"),
    ):
        _install_inputs(spark, monkeypatch, air, weather)
        with pytest.raises(ValueError, match=reason):
            _report(
                spark, locations=ACTIVE[:1], hours=1, end_at=START + timedelta(hours=1)
            )


def test_report_bounds_and_mapping_fail_before_reading_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = START + timedelta(hours=3, minutes=30)
    assert insights.report_period(1, None, now) == (
        START + timedelta(hours=2),
        START + timedelta(hours=3),
    )
    for hours, end in (
        (0, None),
        (745, None),
        (True, None),
        (1, START.replace(tzinfo=None)),
        (1, now),
        (1, START + timedelta(days=1)),
    ):
        with pytest.raises(ValueError):
            insights.report_period(hours, end, now)
    reader = Mock(side_effect=AssertionError("Unexpected table read"))
    monkeypatch.setattr(insights, "_read_snapshot", reader)
    for locations, weather in (
        (ACTIVE[:1], ()),
        (ACTIVE[:1], (WEATHER_LOCATIONS[0],) * 2),
        (ACTIVE[:1] * 2, WEATHER_LOCATIONS),
        ((), WEATHER_LOCATIONS),
    ):
        with pytest.raises(ValueError):
            insights.collect_hourly_environmental_report(
                MagicMock(spec=SparkSession), locations, weather, now=now
            )
    reader.assert_not_called()


def test_cli_stops_spark_on_success_and_failure(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    session = MagicMock(spec=SparkSession)
    monkeypatch.setattr("sys.argv", ["hourly_environmental_insights", "--hours", "1"])
    monkeypatch.setattr(insights, "create_spark_session", lambda _name: session)
    collect = Mock(return_value={"version": 1})
    monkeypatch.setattr(insights, "collect_hourly_environmental_report", collect)
    insights.main()
    assert '"version": 1' in capsys.readouterr().out
    session.stop.assert_called_once()
    session.stop.reset_mock()
    collect.side_effect = RuntimeError("Missing table")
    with pytest.raises(RuntimeError, match="Missing table"):
        insights.main()
    session.stop.assert_called_once()
    for args in (
        ["--hours", "0"],
        ["--end-at", "2026-11-03T10:00:00"],
        ["--weather-snapshot-id", "-1"],
    ):
        monkeypatch.setattr("sys.argv", ["hourly_environmental_insights", *args])
        with pytest.raises(SystemExit) as error:
            insights.main()
        assert error.value.code == 2
