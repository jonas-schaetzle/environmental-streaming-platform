import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

from environmental_streaming.ingestion.openaq_producer import (
    DEFAULT_LOCATIONS_PATH,
    OpenAQLocation,
    load_locations,
)
from environmental_streaming.lakehouse.tables import (
    ICEBERG_CANONICAL_TABLE,
    ICEBERG_HOURLY_AGGREGATE_TABLE,
)
from environmental_streaming.runtime.spark import create_spark_session


DEFAULT_FRESHNESS_THRESHOLD_MINUTES = 120
OPENAQ_SOURCE = "openaq"


@dataclass(frozen=True)
class AirQualityInsightConfig:
    locations_path: Path = DEFAULT_LOCATIONS_PATH
    freshness_threshold_minutes: int = DEFAULT_FRESHNESS_THRESHOLD_MINUTES


def select_latest_measurements(measurements: DataFrame) -> DataFrame:
    latest_first = Window.partitionBy("location_id", "parameter").orderBy(
        col("measured_at_utc").desc(),
        col("kafka_timestamp").desc(),
        col("kafka_partition").desc(),
        col("kafka_offset").desc(),
    )

    return (
        measurements.withColumn("latest_rank", row_number().over(latest_first))
        .where(col("latest_rank") == 1)
        .drop("latest_rank")
    )


def select_latest_hourly_aggregates(aggregates: DataFrame) -> DataFrame:
    latest_first = Window.partitionBy(
        "location_id",
        "parameter",
        "unit",
    ).orderBy(
        col("window_start").desc(),
        col("window_end").desc(),
    )

    return (
        aggregates.withColumn("latest_rank", row_number().over(latest_first))
        .where(col("latest_rank") == 1)
        .drop("latest_rank")
    )


def measurement_freshness(
    measured_at_utc: datetime | None,
    now: datetime,
    threshold_seconds: int,
) -> tuple[str, int | None]:
    if measured_at_utc is None:
        return "missing", None

    measured_at = _as_utc(measured_at_utc)
    current_time = _as_utc(now)
    age_seconds = max(0, int((current_time - measured_at).total_seconds()))
    status = "fresh" if age_seconds <= threshold_seconds else "stale"
    return status, age_seconds


def build_air_quality_report(
    locations: tuple[OpenAQLocation, ...],
    measurement_rows: list[dict[str, Any]],
    aggregate_rows: list[dict[str, Any]],
    *,
    now: datetime,
    freshness_threshold_seconds: int,
) -> dict[str, Any]:
    measurements_by_location: dict[int, list[dict[str, Any]]] = {}
    for row in measurement_rows:
        measurements_by_location.setdefault(row["location_id"], []).append(row)

    aggregates_by_key = {
        (row["location_id"], row["parameter"], row["unit"]): row
        for row in aggregate_rows
    }

    location_reports = []
    for location in locations:
        parameter_rows = sorted(
            measurements_by_location.get(location.location_id, []),
            key=lambda row: row["parameter"],
        )
        parameter_reports = [
            _build_parameter_report(
                row,
                aggregates_by_key.get(
                    (row["location_id"], row["parameter"], row["unit"])
                ),
                now=now,
                freshness_threshold_seconds=freshness_threshold_seconds,
            )
            for row in parameter_rows
        ]
        latest_measured_at = max(
            (row["measured_at_utc"] for row in parameter_rows),
            default=None,
        )
        status, age_seconds = measurement_freshness(
            latest_measured_at,
            now,
            freshness_threshold_seconds,
        )

        location_reports.append(
            {
                "id": location.location_id,
                "name": location.name,
                "status": status,
                "latest_measured_at_utc": _serialize_timestamp(latest_measured_at),
                "freshness_seconds": age_seconds,
                "fresh_parameter_count": sum(
                    parameter["status"] == "fresh"
                    for parameter in parameter_reports
                ),
                "stale_parameter_count": sum(
                    parameter["status"] == "stale"
                    for parameter in parameter_reports
                ),
                "parameters": parameter_reports,
            }
        )

    return {
        "generated_at_utc": _serialize_timestamp(now),
        "freshness_threshold_seconds": freshness_threshold_seconds,
        "locations_configured": len(locations),
        "locations_with_data": sum(
            location["status"] != "missing" for location in location_reports
        ),
        "locations": location_reports,
    }


def collect_air_quality_report(
    spark: SparkSession,
    locations: tuple[OpenAQLocation, ...],
    *,
    now: datetime | None = None,
    freshness_threshold_seconds: int,
) -> dict[str, Any]:
    configured_ids = [location.location_id for location in locations]
    measurements = (
        spark.table(ICEBERG_CANONICAL_TABLE)
        .where(col("source") == OPENAQ_SOURCE)
        .where(col("location_id").isin(configured_ids))
    )
    aggregates = (
        spark.table(ICEBERG_HOURLY_AGGREGATE_TABLE)
        .where(col("source") == OPENAQ_SOURCE)
        .where(col("location_id").isin(configured_ids))
    )

    measurement_rows = [
        row.asDict(recursive=True)
        for row in select_latest_measurements(measurements).collect()
    ]
    aggregate_rows = [
        row.asDict(recursive=True)
        for row in select_latest_hourly_aggregates(aggregates).collect()
    ]

    return build_air_quality_report(
        locations,
        measurement_rows,
        aggregate_rows,
        now=now or datetime.now(timezone.utc),
        freshness_threshold_seconds=freshness_threshold_seconds,
    )


def _build_parameter_report(
    measurement: dict[str, Any],
    aggregate: dict[str, Any] | None,
    *,
    now: datetime,
    freshness_threshold_seconds: int,
) -> dict[str, Any]:
    status, age_seconds = measurement_freshness(
        measurement["measured_at_utc"],
        now,
        freshness_threshold_seconds,
    )

    return {
        "parameter": measurement["parameter"],
        "display_name": measurement["parameter_display_name"],
        "value": measurement["value"],
        "unit": measurement["unit"],
        "measured_at_utc": _serialize_timestamp(measurement["measured_at_utc"]),
        "freshness_seconds": age_seconds,
        "status": status,
        "latest_finalized_hour": (
            {
                "window_start_utc": _serialize_timestamp(aggregate["window_start"]),
                "window_end_utc": _serialize_timestamp(aggregate["window_end"]),
                "measurement_count": aggregate["measurement_count"],
                "average_value": aggregate["average_value"],
                "minimum_value": aggregate["minimum_value"],
                "maximum_value": aggregate["maximum_value"],
                "latest_measured_at_utc": _serialize_timestamp(
                    aggregate["latest_measured_at_utc"]
                ),
            }
            if aggregate is not None
            else None
        ),
    }


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _serialize_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _as_utc(value).isoformat().replace("+00:00", "Z")


def parse_args() -> AirQualityInsightConfig:
    parser = argparse.ArgumentParser(
        description="Report current air-quality measurements from Iceberg."
    )
    parser.add_argument(
        "--locations-path",
        type=Path,
        default=DEFAULT_LOCATIONS_PATH,
        help="JSON file containing the curated OpenAQ locations.",
    )
    parser.add_argument(
        "--freshness-threshold-minutes",
        type=int,
        default=DEFAULT_FRESHNESS_THRESHOLD_MINUTES,
        help="Maximum measurement age that is classified as fresh.",
    )
    args = parser.parse_args()

    if args.freshness_threshold_minutes <= 0:
        parser.error("--freshness-threshold-minutes must be greater than zero")

    return AirQualityInsightConfig(
        locations_path=args.locations_path,
        freshness_threshold_minutes=args.freshness_threshold_minutes,
    )


def main() -> None:
    config = parse_args()
    locations = load_locations(config.locations_path)
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        report = collect_air_quality_report(
            spark,
            locations,
            freshness_threshold_seconds=(
                config.freshness_threshold_minutes * 60
            ),
        )
    finally:
        spark.stop()

    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
