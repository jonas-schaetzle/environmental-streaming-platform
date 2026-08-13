from collections.abc import Generator

import pytest
from pyspark.sql import SparkSession

from src.air_quality_batch import aggregate_air_quality, filter_valid_readings


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession]:
    session = (
        SparkSession.builder
        .appName("air-quality-tests")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")

    yield session

    session.stop()


def test_filter_valid_readings_removes_null_and_negative_values(
    spark: SparkSession,
) -> None:
    readings = spark.createDataFrame(
        [
            ("Berlin", "pm25", 14.2, "ug/m3"),
            ("Berlin", "pm25", -1.0, "ug/m3"),
            ("Hamburg", "no2", None, "ug/m3"),
        ],
        ["city", "measurement_type", "value", "unit"],
    )

    result = filter_valid_readings(readings)

    actual = result.collect()

    assert len(actual) == 1
    assert actual[0]["city"] == "Berlin"
    assert actual[0]["value"] == 14.2


def test_aggregate_air_quality_groups_by_city_measurement_and_unit(
    spark: SparkSession,
) -> None:
    readings = spark.createDataFrame(
        [
            ("Berlin", "pm25", 14.2, "ug/m3"),
            ("Berlin", "pm25", 18.8, "ug/m3"),
            ("Berlin", "no2", 31.5, "ug/m3"),
            ("Hamburg", "pm25", 9.8, "ug/m3"),
        ],
        ["city", "measurement_type", "value", "unit"],
    )

    result = aggregate_air_quality(readings)

    actual = {
        (row["city"], row["measurement_type"], row["unit"]): {
            "reading_count": row["reading_count"],
            "avg_value": row["avg_value"],
            "min_value": row["min_value"],
            "max_value": row["max_value"],
        }
        for row in result.collect()
    }

    assert actual == {
        ("Berlin", "no2", "ug/m3"): {
            "reading_count": 1,
            "avg_value": 31.5,
            "min_value": 31.5,
            "max_value": 31.5,
        },
        ("Berlin", "pm25", "ug/m3"): {
            "reading_count": 2,
            "avg_value": 16.5,
            "min_value": 14.2,
            "max_value": 18.8,
        },
        ("Hamburg", "pm25", "ug/m3"): {
            "reading_count": 1,
            "avg_value": 9.8,
            "min_value": 9.8,
            "max_value": 9.8,
        },
    }
