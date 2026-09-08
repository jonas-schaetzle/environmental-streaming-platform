from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.openaq_latest_batch import (
    enrich_latest_measurements,
    filter_complete_measurements,
    flatten_latest_measurements,
    flatten_sensor_metadata,
)


def test_flatten_latest_measurements_extracts_expected_columns(
    spark: SparkSession,
) -> None:
    raw_schema = StructType(
        [
            StructField(
                "results",
                ArrayType(
                    StructType(
                        [
                            StructField("locationsId", LongType(), nullable=False),
                            StructField("sensorsId", LongType(), nullable=False),
                            StructField("value", DoubleType(), nullable=False),
                            StructField(
                                "datetime",
                                StructType(
                                    [
                                        StructField("utc", StringType(), nullable=False),
                                        StructField("local", StringType(), nullable=False),
                                    ]
                                ),
                                nullable=False,
                            ),
                            StructField(
                                "coordinates",
                                StructType(
                                    [
                                        StructField("latitude", DoubleType(), nullable=False),
                                        StructField("longitude", DoubleType(), nullable=False),
                                    ]
                                ),
                                nullable=False,
                            ),
                        ]
                    )
                ),
                nullable=False,
            )
        ]
    )

    raw = spark.createDataFrame(
        [
            (
                [
                    (
                        123,
                        456,
                        14.2,
                        ("2026-08-14T10:00:00Z", "2026-08-14T12:00:00+02:00"),
                        (52.52, 13.405),
                    )
                ],
            )
        ],
        raw_schema,
    )

    result = flatten_latest_measurements(raw)

    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["location_id"] == 123
    assert rows[0]["sensor_id"] == 456
    assert rows[0]["value"] == 14.2
    assert rows[0]["measured_at_utc_raw"] == "2026-08-14T10:00:00Z"
    assert rows[0]["latitude"] == 52.52
    assert rows[0]["longitude"] == 13.405


def test_flatten_sensor_metadata_extracts_parameter_fields(
    spark: SparkSession,
) -> None:
    raw_schema = StructType(
        [
            StructField(
                "results",
                ArrayType(
                    StructType(
                        [
                            StructField("id", LongType(), nullable=False),
                            StructField(
                                "parameter",
                                StructType(
                                    [
                                        StructField("name", StringType(), nullable=False),
                                        StructField("units", StringType(), nullable=False),
                                        StructField(
                                            "displayName",
                                            StringType(),
                                            nullable=False,
                                        ),
                                    ]
                                ),
                                nullable=False,
                            ),
                        ]
                    )
                ),
                nullable=False,
            )
        ]
    )

    raw = spark.createDataFrame(
        [
            (
                [
                    (
                        456,
                        ("so2", "ppm", "SO2"),
                    )
                ],
            )
        ],
        raw_schema,
    )

    result = flatten_sensor_metadata(raw)

    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["sensor_id"] == 456
    assert rows[0]["parameter"] == "so2"
    assert rows[0]["unit"] == "ppm"
    assert rows[0]["parameter_display_name"] == "SO2"


def test_enrich_latest_measurements_adds_sensor_metadata(
    spark: SparkSession,
) -> None:
    latest_schema = StructType(
        [
            StructField("location_id", LongType(), nullable=False),
            StructField("sensor_id", LongType(), nullable=False),
            StructField("value", DoubleType(), nullable=False),
            StructField("measured_at_utc_raw", StringType(), nullable=False),
            StructField("measured_at_utc", TimestampType(), nullable=True),
            StructField("latitude", DoubleType(), nullable=False),
            StructField("longitude", DoubleType(), nullable=False),
        ]
    )

    metadata_schema = StructType(
        [
            StructField("sensor_id", LongType(), nullable=False),
            StructField("parameter", StringType(), nullable=False),
            StructField("unit", StringType(), nullable=False),
            StructField("parameter_display_name", StringType(), nullable=False),
        ]
    )

    latest_measurements = spark.createDataFrame(
        [
            (
                123,
                456,
                0.0004,
                "2026-08-14T12:00:00Z",
                None,
                35.1353,
                -106.584702,
            )
        ],
        latest_schema,
    )

    sensor_metadata = spark.createDataFrame(
        [
            (
                456,
                "so2",
                "ppm",
                "SO2",
            )
        ],
        metadata_schema,
    )

    result = enrich_latest_measurements(latest_measurements, sensor_metadata)

    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["source"] == "openaq"
    assert rows[0]["location_id"] == 123
    assert rows[0]["sensor_id"] == 456
    assert rows[0]["parameter"] == "so2"
    assert rows[0]["parameter_display_name"] == "SO2"
    assert rows[0]["value"] == 0.0004
    assert rows[0]["unit"] == "ppm"


def test_filter_complete_measurements_removes_missing_metadata(
    spark: SparkSession,
) -> None:
    measurements = spark.createDataFrame(
        [
            (123, 456, "so2", "SO2", 0.0004, "ppm"),
            (123, 999, None, None, 12.3, None),
        ],
        [
            "location_id",
            "sensor_id",
            "parameter",
            "parameter_display_name",
            "value",
            "unit",
        ],
    )

    result = filter_complete_measurements(measurements)

    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["sensor_id"] == 456
    assert rows[0]["parameter"] == "so2"
    assert rows[0]["unit"] == "ppm"
