from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.measurement_window_stream import aggregate_measurements_by_window


def test_aggregate_measurements_by_window_counts_and_averages_valid_events(
    spark: SparkSession,
) -> None:
    schema = StructType(
        [
            StructField("source", StringType(), nullable=False),
            StructField("location_id", LongType(), nullable=False),
            StructField("sensor_id", LongType(), nullable=False),
            StructField("parameter", StringType(), nullable=True),
            StructField("parameter_display_name", StringType(), nullable=True),
            StructField("value", DoubleType(), nullable=True),
            StructField("unit", StringType(), nullable=True),
            StructField("measured_at_utc", TimestampType(), nullable=True),
            StructField("latitude", DoubleType(), nullable=True),
            StructField("longitude", DoubleType(), nullable=True),
        ]
    )
    measurements = spark.createDataFrame(
        [
            (
                "openaq",
                2178,
                3916,
                "no2",
                "NO2",
                0.006,
                "ppm",
                datetime(2026, 8, 13, 15, 0, 0),
                35.1353,
                -106.584702,
            ),
            (
                "openaq",
                2178,
                3916,
                "no2",
                "NO2",
                0.008,
                "ppm",
                datetime(2026, 8, 13, 15, 2, 0),
                35.1353,
                -106.584702,
            ),
            (
                "openaq",
                2178,
                3918,
                "so2",
                "SO2",
                -0.1,
                "ppm",
                datetime(2026, 8, 13, 15, 2, 0),
                35.1353,
                -106.584702,
            ),
        ],
        schema,
    )

    result = aggregate_measurements_by_window(measurements)

    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["source"] == "openaq"
    assert rows[0]["parameter"] == "no2"
    assert rows[0]["unit"] == "ppm"
    assert rows[0]["measurement_count"] == 2
    assert rows[0]["avg_value"] == 0.007
