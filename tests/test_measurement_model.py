from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType, TimestampType

from src.measurement_model import (
    canonical_measurement_schema,
    filter_invalid_measurements,
    filter_valid_measurements,
    normalize_measurement_units,
)


def test_canonical_measurement_schema_matches_contract() -> None:
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


def test_validation_routes_events_and_preserves_error_reasons(
    spark: SparkSession,
) -> None:
    measurements = spark.createDataFrame(
        [
            ("openaq", 2178, 3916, "no2", 0.007, "ppm", "2026-08-13 15:00:00"),
            ("openaq", 2178, 3918, "so2", -0.1, None, "2026-08-13 15:00:00"),
        ],
        [
            "source",
            "location_id",
            "sensor_id",
            "parameter",
            "value",
            "unit",
            "measured_at_utc",
        ],
    )

    valid_rows = filter_valid_measurements(measurements).collect()
    invalid_rows = filter_invalid_measurements(measurements).collect()

    assert [row["sensor_id"] for row in valid_rows] == [3916]
    assert [row["sensor_id"] for row in invalid_rows] == [3918]
    assert invalid_rows[0]["validation_error"] == "negative_value,missing_unit"


def test_normalize_measurement_units_standardizes_micrograms_per_cubic_meter(
    spark: SparkSession,
) -> None:
    measurements = spark.createDataFrame(
        [(3916, "ppm"), (3919, "ug/m3"), (3920, "µg/m³")],
        ["sensor_id", "unit"],
    )

    rows = {
        row["sensor_id"]: row["unit"]
        for row in normalize_measurement_units(measurements).collect()
    }

    assert rows == {3916: "ppm", 3919: "µg/m³", 3920: "µg/m³"}
