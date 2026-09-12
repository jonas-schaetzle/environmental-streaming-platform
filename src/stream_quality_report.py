from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructType

if __package__:
    from .measurement_file_stream import (
        INVALID_STREAM_OUTPUT_PATH,
        STREAM_OUTPUT_PATH,
        canonical_measurement_schema,
    )
else:
    from measurement_file_stream import (
        INVALID_STREAM_OUTPUT_PATH,
        STREAM_OUTPUT_PATH,
        canonical_measurement_schema,
    )


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("stream-quality-report")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def invalid_measurement_schema() -> StructType:
    return canonical_measurement_schema().add(
        "validation_error",
        StringType(),
        nullable=False,
    )


def calculate_quality_metrics(
    valid_measurements: DataFrame,
    invalid_measurements: DataFrame,
) -> dict[str, Any]:
    valid_count = valid_measurements.count()
    invalid_count = invalid_measurements.count()
    total_count = valid_count + invalid_count
    invalid_rate = invalid_count / total_count if total_count else 0.0

    invalid_by_error = {
        row["validation_error"]: row["count"]
        for row in invalid_measurements.groupBy("validation_error").count().collect()
    }

    return {
        "valid_count": valid_count,
        "invalid_count": invalid_count,
        "total_count": total_count,
        "invalid_rate": invalid_rate,
        "invalid_by_error": invalid_by_error,
    }


def format_quality_report(metrics: dict[str, Any]) -> str:
    lines = [
        "Stream Quality Report",
        "---------------------",
        f"Total events:   {metrics['total_count']}",
        f"Valid events:   {metrics['valid_count']}",
        f"Invalid events: {metrics['invalid_count']}",
        f"Invalid rate:   {metrics['invalid_rate']:.2%}",
    ]

    invalid_by_error = metrics["invalid_by_error"]
    if invalid_by_error:
        lines.append("")
        lines.append("Invalid events by reason:")
        for reason, count in sorted(invalid_by_error.items()):
            lines.append(f"- {reason}: {count}")

    return "\n".join(lines)


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    valid_measurements = spark.read.schema(canonical_measurement_schema()).parquet(
        STREAM_OUTPUT_PATH
    )
    invalid_measurements = spark.read.schema(invalid_measurement_schema()).parquet(
        INVALID_STREAM_OUTPUT_PATH
    )

    metrics = calculate_quality_metrics(valid_measurements, invalid_measurements)
    print(format_quality_report(metrics))

    spark.stop()


if __name__ == "__main__":
    main()
