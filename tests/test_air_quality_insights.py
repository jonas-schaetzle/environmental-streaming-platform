from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.air_quality_insights import (
    build_air_quality_report,
    measurement_freshness,
    select_latest_hourly_aggregates,
    select_latest_measurements,
)
from src.openaq_kafka_producer import OpenAQLocation


def test_measurement_freshness_handles_boundaries_and_missing_values() -> None:
    now = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)

    assert measurement_freshness(None, now, 7200) == ("missing", None)
    assert measurement_freshness(
        datetime(2026, 9, 28, 10, 0), now, 7200
    ) == ("fresh", 7200)
    assert measurement_freshness(
        datetime(2026, 9, 28, 9, 59, 59), now, 7200
    ) == ("stale", 7201)
    assert measurement_freshness(
        datetime(2026, 9, 28, 12, 1), now, 7200
    ) == ("fresh", 0)


def test_latest_rows_are_selected_deterministically(spark: SparkSession) -> None:
    measurement_schema = StructType(
        [
            StructField("location_id", LongType(), nullable=False),
            StructField("parameter", StringType(), nullable=False),
            StructField("measured_at_utc", TimestampType(), nullable=False),
            StructField("kafka_timestamp", TimestampType(), nullable=False),
            StructField("kafka_partition", IntegerType(), nullable=False),
            StructField("kafka_offset", LongType(), nullable=False),
            StructField("value", DoubleType(), nullable=False),
        ]
    )
    measurements = spark.createDataFrame(
        [
            (
                2669,
                "pm25",
                datetime(2026, 9, 28, 10),
                datetime(2026, 9, 28, 10),
                0,
                3,
                10.0,
            ),
            (
                2669,
                "pm25",
                datetime(2026, 9, 28, 11),
                datetime(2026, 9, 28, 11),
                0,
                4,
                11.0,
            ),
            (
                2669,
                "pm25",
                datetime(2026, 9, 28, 11),
                datetime(2026, 9, 28, 11),
                0,
                5,
                12.0,
            ),
            (
                2669,
                "no2",
                datetime(2026, 9, 28, 9),
                datetime(2026, 9, 28, 9),
                1,
                1,
                7.0,
            ),
        ],
        measurement_schema,
    )
    aggregate_schema = StructType(
        [
            StructField("location_id", LongType(), nullable=False),
            StructField("parameter", StringType(), nullable=False),
            StructField("unit", StringType(), nullable=False),
            StructField("window_start", TimestampType(), nullable=False),
            StructField("window_end", TimestampType(), nullable=False),
            StructField("average_value", DoubleType(), nullable=False),
        ]
    )
    aggregates = spark.createDataFrame(
        [
            (
                2669,
                "pm25",
                "ug/m3",
                datetime(2026, 9, 28, 9),
                datetime(2026, 9, 28, 10),
                9.0,
            ),
            (
                2669,
                "pm25",
                "ug/m3",
                datetime(2026, 9, 28, 10),
                datetime(2026, 9, 28, 11),
                11.0,
            ),
        ],
        aggregate_schema,
    )

    latest_measurements = {
        row["parameter"]: row
        for row in select_latest_measurements(measurements).collect()
    }
    latest_aggregate = select_latest_hourly_aggregates(aggregates).first()

    assert latest_measurements["pm25"]["kafka_offset"] == 5
    assert latest_measurements["pm25"]["value"] == 12.0
    assert latest_measurements["no2"]["value"] == 7.0
    assert latest_aggregate["window_start"] == datetime(2026, 9, 28, 10)
    assert latest_aggregate["average_value"] == 11.0


def test_report_includes_latest_hour_and_configured_location_without_data() -> None:
    locations = (
        OpenAQLocation(location_id=2669, name="Munich Stachus"),
        OpenAQLocation(location_id=2936, name="Stuttgart Bad Cannstatt"),
    )
    now = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)
    measurements = [
        {
            "location_id": 2669,
            "parameter": "pm25",
            "parameter_display_name": "PM2.5",
            "value": 12.0,
            "unit": "ug/m3",
            "measured_at_utc": datetime(2026, 9, 28, 11, 30),
        },
        {
            "location_id": 2669,
            "parameter": "no2",
            "parameter_display_name": "NO2",
            "value": 20.0,
            "unit": "ug/m3",
            "measured_at_utc": datetime(2026, 9, 28, 8, 0),
        },
    ]
    aggregates = [
        {
            "location_id": 2669,
            "parameter": "pm25",
            "unit": "ug/m3",
            "window_start": datetime(2026, 9, 28, 10, 0),
            "window_end": datetime(2026, 9, 28, 11, 0),
            "measurement_count": 4,
            "average_value": 11.5,
            "minimum_value": 9.0,
            "maximum_value": 13.0,
            "latest_measured_at_utc": datetime(2026, 9, 28, 10, 45),
        }
    ]

    report = build_air_quality_report(
        locations,
        measurements,
        aggregates,
        now=now,
        freshness_threshold_seconds=7200,
    )

    munich, stuttgart = report["locations"]
    assert report["locations_with_data"] == 1
    assert munich["status"] == "fresh"
    assert munich["fresh_parameter_count"] == 1
    assert munich["stale_parameter_count"] == 1
    assert [item["parameter"] for item in munich["parameters"]] == ["no2", "pm25"]
    assert munich["parameters"][0]["latest_finalized_hour"] is None
    assert munich["parameters"][1]["latest_finalized_hour"] == {
        "window_start_utc": "2026-09-28T10:00:00Z",
        "window_end_utc": "2026-09-28T11:00:00Z",
        "measurement_count": 4,
        "average_value": 11.5,
        "minimum_value": 9.0,
        "maximum_value": 13.0,
        "latest_measured_at_utc": "2026-09-28T10:45:00Z",
    }
    assert stuttgart["status"] == "missing"
    assert stuttgart["latest_measured_at_utc"] is None
    assert stuttgart["freshness_seconds"] is None
    assert stuttgart["parameters"] == []
