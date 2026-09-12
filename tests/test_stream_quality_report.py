from pyspark.sql import SparkSession

from src.stream_quality_report import calculate_quality_metrics, format_quality_report


def test_quality_report_calculates_counts_rates_and_error_distribution(
    spark: SparkSession,
) -> None:
    valid_measurements = spark.createDataFrame(
        [
            ("openaq", 3916, "no2"),
            ("openaq", 3920, "pm25"),
            ("openaq", 25227, "co"),
        ],
        ["source", "sensor_id", "parameter"],
    )
    invalid_measurements = spark.createDataFrame(
        [
            ("openaq", 3918, "negative_value"),
            ("openaq", 4272103, "missing_unit"),
        ],
        ["source", "sensor_id", "validation_error"],
    )

    metrics = calculate_quality_metrics(valid_measurements, invalid_measurements)
    report = format_quality_report(metrics)

    assert metrics == {
        "valid_count": 3,
        "invalid_count": 2,
        "total_count": 5,
        "invalid_rate": 0.4,
        "invalid_by_error": {
            "missing_unit": 1,
            "negative_value": 1,
        },
    }
    assert "Invalid rate:   40.00%" in report
    assert "- missing_unit: 1" in report
    assert "- negative_value: 1" in report
