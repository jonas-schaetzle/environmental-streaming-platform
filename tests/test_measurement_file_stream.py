from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType, TimestampType

from src.measurement_file_stream import (
    canonical_measurement_schema,
    filter_valid_measurements,
)


def test_canonical_measurement_schema_matches_documented_field_order() -> None:
    schema = canonical_measurement_schema()

    assert schema.fieldNames() == [
        "source",
        "location_id",
        "sensor_id",
        "parameter",
        "parameter_display_name",
        "value",
        "unit",
        "measured_at_utc",
        "latitude",
        "longitude",
    ]

    assert isinstance(schema["source"].dataType, StringType)
    assert isinstance(schema["location_id"].dataType, LongType)
    assert isinstance(schema["sensor_id"].dataType, LongType)
    assert isinstance(schema["value"].dataType, DoubleType)
    assert isinstance(schema["measured_at_utc"].dataType, TimestampType)


def test_filter_valid_measurements_removes_incomplete_or_invalid_events(
    spark: SparkSession,
) -> None:
    measurements = spark.createDataFrame(
        [
            ("openaq", 2178, 3916, "no2", "NO2", 0.007, "ppm", "2026-08-13 15:00:00"),
            ("openaq", 2178, 3917, None, "O3", 0.037, "ppm", "2026-08-13 15:00:00"),
            ("openaq", 2178, 3918, "so2", "SO2", -0.1, "ppm", "2026-08-13 15:00:00"),
            ("openaq", 2178, 3920, "pm25", "PM2.5", 4.0, None, "2026-08-13 15:00:00"),
        ],
        [
            "source",
            "location_id",
            "sensor_id",
            "parameter",
            "parameter_display_name",
            "value",
            "unit",
            "measured_at_utc",
        ],
    )

    result = filter_valid_measurements(measurements)

    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["sensor_id"] == 3916
    assert rows[0]["parameter"] == "no2"
