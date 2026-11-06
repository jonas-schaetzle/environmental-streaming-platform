import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, lit, max as spark_max, struct

from environmental_streaming.ingestion.openaq_producer import (
    DEFAULT_LOCATIONS_PATH,
    load_locations,
)
from environmental_streaming.ingestion.weather_locations import (
    DEFAULT_WEATHER_LOCATIONS_PATH,
    load_weather_locations,
)
from environmental_streaming.lakehouse.tables import (
    ICEBERG_QUARANTINE_TABLE,
    ICEBERG_WEATHER_QUARANTINE_TABLE,
    KAFKA_IDENTITY_COLUMNS,
)
from environmental_streaming.products.air_quality_insights import (
    DEFAULT_FRESHNESS_THRESHOLD_MINUTES,
    collect_air_quality_report,
)
from environmental_streaming.products.hourly_environmental_insights import (
    AIR_PARAMETERS,
    MAX_REPORT_HOURS,
    collect_hourly_environmental_report,
)
from environmental_streaming.runtime.spark import create_spark_session
from environmental_streaming.runtime.timestamps import collect_utc_rows


QUARANTINE_TABLES = (ICEBERG_QUARANTINE_TABLE, ICEBERG_WEATHER_QUARANTINE_TABLE)
COVERAGE_RULES = (
    (
        "air_quality_hours",
        "low_finalized_air_coverage",
        "Check source delivery and watermark progress.",
    ),
    (
        "complete_weather_hours",
        "low_complete_weather_coverage",
        "Check weather collection and which parameters are missing in the hourly report.",
    ),
)


def _iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def summarize_quarantine(
    frame: DataFrame, start: datetime, end: datetime
) -> list[dict[str, Any]]:
    timestamp = "kafka_timestamp"
    recent = frame.where((col(timestamp) >= lit(start)) & (col(timestamp) < lit(end)))
    groups = (
        recent.groupBy("validation_error")
        .agg(
            count(lit(1)).alias("row_count"),
            spark_max(struct(timestamp, *KAFKA_IDENTITY_COLUMNS)).alias("sample"),
        )
        .select("validation_error", "row_count", "sample.*")
        .orderBy("validation_error")
    )
    reasons = []
    for row in collect_utc_rows(groups, (timestamp,)):
        reasons.append(
            {
                "validation_error": row["validation_error"],
                "row_count": row["row_count"],
                "latest_kafka_record": {
                    "timestamp_utc": _iso_utc(row[timestamp]),
                    "topic": row["kafka_topic"],
                    "partition": row["kafka_partition"],
                    "offset": row["kafka_offset"],
                },
            }
        )
    return reasons


def build_alert_report(
    latest: dict[str, Any],
    hourly: dict[str, Any],
    quarantine: dict[str, list[dict[str, Any]]],
    *,
    quarantine_start: datetime,
    now: datetime,
    minimum_coverage_percent: int,
) -> dict[str, Any]:
    parameter = hourly["parameter"]
    latest_by_id = {
        location["id"]: location["parameters"] for location in latest["locations"]
    }
    alerts = []
    for location in hourly["locations"]:
        station = {
            key: location[key] for key in ("location_id", "name", "weather_location_id")
        }
        measurement = next(
            (
                item
                for item in latest_by_id[station["location_id"]]
                if item["parameter"] == parameter
            ),
            None,
        )
        if measurement is None or measurement["status"] == "stale":
            alerts.append(
                {
                    "code": (
                        "missing_air_measurement"
                        if measurement is None
                        else "stale_air_measurement"
                    ),
                    "station": station,
                    "evidence": {
                        "measured_at_utc": (
                            measurement["measured_at_utc"] if measurement else None
                        ),
                        "age_seconds": (
                            measurement["freshness_seconds"] if measurement else None
                        ),
                        "threshold_seconds": latest["freshness_threshold_seconds"],
                    },
                    "action": "Check the station sensor and the OpenAQ polling run.",
                }
            )

        coverage = location["coverage"]
        expected = coverage["expected_hours"]
        for metric, code, action in COVERAGE_RULES:
            if 100 * coverage[metric] < minimum_coverage_percent * expected:
                alerts.append(
                    {
                        "code": code,
                        "station": station,
                        "evidence": {
                            "available_hours": coverage[metric],
                            "expected_hours": expected,
                            "minimum_coverage_percent": minimum_coverage_percent,
                        },
                        "action": action,
                    }
                )

    for table, reasons in quarantine.items():
        if reasons:
            alerts.append(
                {
                    "code": "recent_quarantine",
                    "evidence": {
                        "table": table,
                        "row_count": sum(reason["row_count"] for reason in reasons),
                        "reasons": reasons,
                    },
                    "action": "Inspect the validation reasons and Kafka record identities.",
                }
            )

    return {
        "version": 1,
        "generated_at_utc": _iso_utc(now),
        "status": "alerts" if alerts else "clear",
        "parameter": parameter,
        "freshness_threshold_seconds": latest["freshness_threshold_seconds"],
        "coverage_window": {
            "start_utc": hourly["start_utc"],
            "end_utc": hourly["end_utc"],
            "minimum_coverage_percent": minimum_coverage_percent,
            "inputs": hourly["inputs"],
        },
        "quarantine_window": {
            "start_utc": _iso_utc(quarantine_start),
            "end_utc": _iso_utc(now),
            "time_field": "kafka_timestamp",
            "checked_tables": list(quarantine),
        },
        "alerts": alerts,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Report recent data-quality alerts.")
    parser.add_argument("--parameter", choices=AIR_PARAMETERS, default="pm25")
    parser.add_argument("--coverage-hours", type=int, default=48)
    parser.add_argument("--minimum-coverage-percent", type=int, default=50)
    parser.add_argument(
        "--freshness-threshold-minutes",
        type=int,
        default=DEFAULT_FRESHNESS_THRESHOLD_MINUTES,
    )
    parser.add_argument("--quarantine-hours", type=int, default=24)
    parser.add_argument("--locations-path", type=Path, default=DEFAULT_LOCATIONS_PATH)
    parser.add_argument(
        "--weather-locations-path", type=Path, default=DEFAULT_WEATHER_LOCATIONS_PATH
    )
    args = parser.parse_args()
    for option in ("coverage_hours", "quarantine_hours"):
        if not 1 <= getattr(args, option) <= MAX_REPORT_HOURS:
            parser.error(f"--{option.replace('_', '-')} must be 1–{MAX_REPORT_HOURS}")
    if not 1 <= args.minimum_coverage_percent <= 100:
        parser.error("--minimum-coverage-percent must be 1–100")
    if args.freshness_threshold_minutes <= 0:
        parser.error("--freshness-threshold-minutes must be positive")

    locations = load_locations(args.locations_path)
    weather_locations = load_weather_locations(args.weather_locations_path)
    spark = create_spark_session("data-quality-alerts")
    try:
        spark.sparkContext.setLogLevel("WARN")
        now = datetime.now(timezone.utc)
        quarantine_start = now - timedelta(hours=args.quarantine_hours)
        hourly = collect_hourly_environmental_report(
            spark,
            locations,
            weather_locations,
            parameter=args.parameter,
            hours=args.coverage_hours,
            now=now,
        )
        latest = collect_air_quality_report(
            spark,
            locations,
            now=now,
            freshness_threshold_seconds=args.freshness_threshold_minutes * 60,
        )
        quarantine = {
            table: summarize_quarantine(spark.table(table), quarantine_start, now)
            for table in QUARANTINE_TABLES
        }
    finally:
        spark.stop()
    report = build_alert_report(
        latest,
        hourly,
        quarantine,
        quarantine_start=quarantine_start,
        now=now,
        minimum_coverage_percent=args.minimum_coverage_percent,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
