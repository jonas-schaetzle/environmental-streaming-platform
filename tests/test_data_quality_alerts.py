from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

from environmental_streaming.products.data_quality_alerts import (
    QUARANTINE_TABLES,
    build_alert_report,
    summarize_quarantine,
)


NOW = datetime(2026, 11, 6, 14, 15, tzinfo=timezone.utc)
QUARANTINE_START = NOW - timedelta(days=1)


def _station(location_id: int, air_hours: int, weather_hours: int) -> dict:
    return {
        "location_id": location_id,
        "name": f"Station {location_id}",
        "weather_location_id": f"weather-{location_id}",
        "coverage": {
            "expected_hours": 4,
            "air_quality_hours": air_hours,
            "complete_weather_hours": weather_hours,
        },
    }


def test_alerts_keep_latest_measurements_and_finalized_coverage_separate() -> None:
    fresh = {"parameter": "pm25", "status": "fresh"}
    stale = {
        "parameter": "pm25",
        "status": "stale",
        "measured_at_utc": "2026-11-06T12:14:59Z",
        "freshness_seconds": 7201,
    }
    latest = {
        "freshness_threshold_seconds": 7200,
        "locations": [
            {"id": 1, "parameters": [fresh]},
            {"id": 2, "parameters": [stale]},
            {"id": 3, "parameters": [{"parameter": "no2", "status": "fresh"}]},
        ],
    }
    hourly = {
        "parameter": "pm25",
        "start_utc": "2026-11-06T10:00:00Z",
        "end_utc": "2026-11-06T14:00:00Z",
        "inputs": {"air_quality": {"snapshot_id": 123}},
        "locations": [_station(1, 1, 2), _station(2, 2, 1), _station(3, 2, 2)],
    }
    quarantine = {
        QUARANTINE_TABLES[0]: [],
        QUARANTINE_TABLES[1]: [{"validation_error": "invalid_payload", "row_count": 2}],
    }
    report = build_alert_report(
        latest,
        hourly,
        quarantine,
        quarantine_start=QUARANTINE_START,
        now=NOW,
        minimum_coverage_percent=50,
    )

    assert report["status"] == "alerts"
    assert report["coverage_window"]["inputs"] == hourly["inputs"]
    assert report["quarantine_window"]["checked_tables"] == list(QUARANTINE_TABLES)
    air, stale_alert, weather, missing, quarantined = report["alerts"]
    assert air["code"] == "low_finalized_air_coverage"
    assert air["station"]["location_id"] == 1
    assert air["evidence"] == {
        "available_hours": 1,
        "expected_hours": 4,
        "minimum_coverage_percent": 50,
    }
    assert stale_alert["code"] == "stale_air_measurement"
    assert stale_alert["evidence"]["age_seconds"] == 7201
    assert stale_alert["evidence"]["threshold_seconds"] == 7200
    assert weather["code"] == "low_complete_weather_coverage"
    assert weather["station"]["weather_location_id"] == "weather-2"
    assert missing["code"] == "missing_air_measurement"
    assert missing["station"]["location_id"] == 3
    assert missing["evidence"]["measured_at_utc"] is None
    assert quarantined["code"] == "recent_quarantine"
    assert quarantined["evidence"]["table"] == QUARANTINE_TABLES[1]
    assert quarantined["evidence"]["row_count"] == 2

    hourly["locations"] = [_station(1, 2, 2)]
    quarantine[QUARANTINE_TABLES[1]] = []
    clear = build_alert_report(
        latest,
        hourly,
        quarantine,
        quarantine_start=QUARANTINE_START,
        now=NOW,
        minimum_coverage_percent=50,
    )
    assert clear["status"] == "clear"
    assert clear["alerts"] == []


def test_quarantine_window_groups_reasons_and_keeps_latest_kafka_identity(
    spark: SparkSession, process_timezone: str
) -> None:
    just_before_end = NOW - timedelta(microseconds=1)
    schema = (
        "kafka_timestamp timestamp, kafka_topic string, kafka_partition int, "
        "kafka_offset long, validation_error string, raw_value string"
    )
    rows = [
        (QUARANTINE_START - timedelta(microseconds=1), "weather", 0, 1, "old", "raw"),
        (QUARANTINE_START, "weather", 0, 2, "invalid_payload", "raw"),
        (just_before_end, "weather", 0, 3, "invalid_payload", "raw"),
        (just_before_end, "weather", 1, 4, "invalid_payload", "raw"),
        (just_before_end, "weather", 0, 5, "invalid_time", "raw"),
        (NOW, "weather", 0, 6, "outside", "raw"),
        (None, "weather", 0, 7, "no_timestamp", "raw"),
    ]
    frame = spark.createDataFrame(rows, schema)

    invalid_payload, invalid_time = summarize_quarantine(frame, QUARANTINE_START, NOW)

    assert invalid_payload == {
        "validation_error": "invalid_payload",
        "row_count": 3,
        "latest_kafka_record": {
            "timestamp_utc": "2026-11-06T14:14:59.999999Z",
            "topic": "weather",
            "partition": 1,
            "offset": 4,
        },
    }
    assert invalid_time["validation_error"] == "invalid_time"
    assert invalid_time["row_count"] == 1
    assert (
        summarize_quarantine(frame, NOW + timedelta(hours=1), NOW + timedelta(hours=2))
        == []
    )
