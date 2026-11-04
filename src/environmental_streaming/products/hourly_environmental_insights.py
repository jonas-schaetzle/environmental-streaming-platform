import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    collect_list,
    lit,
    sort_array,
    struct,
    unix_micros,
    when,
)

from environmental_streaming.ingestion.openaq_producer import (
    DEFAULT_LOCATIONS_PATH,
    OpenAQLocation,
    load_locations,
)
from environmental_streaming.ingestion.weather_locations import (
    DEFAULT_WEATHER_LOCATIONS_PATH,
    WeatherLocation,
    load_weather_locations,
)
from environmental_streaming.lakehouse.tables import (
    ICEBERG_HOURLY_AGGREGATE_TABLE,
    ICEBERG_WEATHER_CANONICAL_TABLE,
)
from environmental_streaming.processing.weather_model import (
    WEATHER_PARAMETER_UNITS,
    WEATHER_SOURCE,
    WEATHER_SOURCE_DATASET,
)
from environmental_streaming.runtime.spark import create_spark_session
from environmental_streaming.runtime.timestamps import utc_datetime_from_micros


AIR_PARAMETERS = ("pm25", "pm10", "no2", "o3")
AIR_UNIT = "µg/m³"
MAX_REPORT_HOURS = 31 * 24
HOUR_MICROS = 3_600_000_000


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Report timestamps must be timezone-aware.")
    return value.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def _iso_micros(value: int) -> str:
    return _iso(utc_datetime_from_micros(value))


def report_period(
    hours: int, end_at: datetime | None, now: datetime
) -> tuple[datetime, datetime]:
    if type(hours) is not int or not 1 <= hours <= MAX_REPORT_HOURS:
        raise ValueError(f"hours must be between 1 and {MAX_REPORT_HOURS}.")
    completed = _utc(now).replace(minute=0, second=0, microsecond=0)
    end = completed if end_at is None else _utc(end_at)
    if end != end.replace(minute=0, second=0, microsecond=0) or end > completed:
        raise ValueError("end_at must be a completed UTC hour boundary.")
    return end - timedelta(hours=hours), end


def _station_rows(
    locations: tuple[OpenAQLocation, ...],
    weather_locations: tuple[WeatherLocation, ...],
) -> list[tuple[int, str, str, str]]:
    if not locations or len({loc.location_id for loc in locations}) != len(locations):
        raise ValueError("Active locations must be non-empty and unique.")
    rows = []
    for location in locations:
        matches = [
            weather
            for weather in weather_locations
            if location.location_id in weather.openaq_location_ids
        ]
        if len(matches) != 1:
            raise ValueError(
                f"Location {location.location_id} needs one weather mapping."
            )
        weather = matches[0]
        rows.append(
            (
                location.location_id,
                location.name,
                weather.city,
                weather.weather_location_id,
            )
        )
    return rows


def join_hourly_environment(
    aggregates: DataFrame,
    weather: DataFrame,
    locations: tuple[OpenAQLocation, ...],
    weather_locations: tuple[WeatherLocation, ...],
    *,
    start: datetime,
    end: datetime,
    parameter: str,
) -> DataFrame:
    """Join trusted finalized aggregates and canonical weather onto a UTC hour grid."""
    if parameter not in AIR_PARAMETERS:
        raise ValueError(f"parameter must be one of {AIR_PARAMETERS}.")
    station_rows = _station_rows(locations, weather_locations)
    start = _utc(start)
    end = _utc(end)
    hours = int((end - start).total_seconds() // 3600)
    if (
        hours < 1
        or hours > MAX_REPORT_HOURS
        or end - start != timedelta(hours=hours)
        or start.minute
        or start.second
        or start.microsecond
    ):
        raise ValueError(
            "The report period must contain whole UTC hours within bounds."
        )
    stations = aggregates.sparkSession.createDataFrame(
        station_rows,
        "location_id long, location_name string, city string, weather_location_id string",
    )
    grid = (
        aggregates.sparkSession.range(hours)
        .select(
            (lit(int(start.timestamp()) * 1_000_000) + col("id") * HOUR_MICROS).alias(
                "window_start_micros"
            )
        )
        .withColumn("window_end_micros", col("window_start_micros") + HOUR_MICROS)
    )
    air = aggregates.where(
        (col("source") == "openaq")
        & (col("parameter") == parameter)
        & (col("unit") == AIR_UNIT)
        & (col("window_start") >= lit(start))
        & (col("window_end") <= lit(end))
    ).select(
        "location_id",
        unix_micros("window_start").alias("window_start_micros"),
        unix_micros("window_end").alias("window_end_micros"),
        struct(
            "measurement_count",
            "average_value",
            "minimum_value",
            "maximum_value",
            unix_micros("latest_measured_at_utc").alias("latest_measured_at_micros"),
        ).alias("air_quality"),
    )
    aligned_weather = weather.where(
        (col("source") == WEATHER_SOURCE)
        & (col("source_dataset") == WEATHER_SOURCE_DATASET)
        & col("parameter").isin(list(WEATHER_PARAMETER_UNITS))
        & (col("measured_at_utc") >= lit(start))
        & (col("measured_at_utc") <= lit(end))
    ).withColumn(
        "window_start_micros",
        unix_micros("measured_at_utc")
        - when(col("parameter") == "precipitation", lit(HOUR_MICROS)).otherwise(lit(0)),
    )
    context = aligned_weather.groupBy("weather_location_id", "window_start_micros").agg(
        sort_array(
            collect_list(
                struct(
                    "parameter",
                    "value",
                    "unit",
                    unix_micros("measured_at_utc").alias("valid_at_micros"),
                    unix_micros("fetched_at_utc").alias("fetched_at_micros"),
                    "latitude",
                    "longitude",
                    "grid_latitude",
                    "grid_longitude",
                    "kafka_topic",
                    "kafka_partition",
                    "kafka_offset",
                )
            )
        ).alias("weather_entries")
    )
    return (
        stations.crossJoin(grid)
        .join(air, ["location_id", "window_start_micros", "window_end_micros"], "left")
        .join(context, ["weather_location_id", "window_start_micros"], "left")
        .orderBy("location_id", "window_start_micros")
    )


def _hour_report(row: dict[str, Any]) -> dict[str, Any]:
    entries = row["weather_entries"] or []
    if len({item["parameter"] for item in entries}) != len(entries):
        raise ValueError("Canonical weather has duplicate parameter/hour identities.")
    weather = dict.fromkeys(sorted(WEATHER_PARAMETER_UNITS))
    for entry in entries:
        weather[entry["parameter"]] = {
            "value": entry["value"],
            "unit": entry["unit"],
            "valid_at_utc": _iso_micros(entry["valid_at_micros"]),
            "fetched_at_utc": _iso_micros(entry["fetched_at_micros"]),
            "latitude": entry["latitude"],
            "longitude": entry["longitude"],
            "grid_latitude": entry["grid_latitude"],
            "grid_longitude": entry["grid_longitude"],
            "kafka": {
                "topic": entry["kafka_topic"],
                "partition": entry["kafka_partition"],
                "offset": entry["kafka_offset"],
            },
        }
    air = row["air_quality"]
    if air is not None:
        air = dict(air)
        air["latest_measured_at_utc"] = _iso_micros(
            air.pop("latest_measured_at_micros")
        )
    return {
        "window_start_utc": _iso_micros(row["window_start_micros"]),
        "window_end_utc": _iso_micros(row["window_end_micros"]),
        "air_quality_status": "available"
        if air is not None
        else "missing_finalized_aggregate",
        "air_quality": air,
        "weather_status": (
            "complete"
            if len(entries) == len(weather)
            else "partial"
            if entries
            else "missing"
        ),
        "weather": weather,
    }


def _read_snapshot(
    spark: SparkSession, table: str, snapshot_id: int | None
) -> tuple[DataFrame, int | None]:
    spark.catalog.refreshTable(table)
    if snapshot_id is None:
        current = spark.table(f"{table}.refs").where(col("name") == "main").first()
        snapshot_id = current["snapshot_id"] if current is not None else None
    if snapshot_id is None:
        return spark.table(table).limit(0), None
    if type(snapshot_id) is not int or snapshot_id <= 0:
        raise ValueError("Snapshot IDs must be positive integers.")
    return (
        spark.read.format("iceberg")
        .option("versionAsOf", str(snapshot_id))
        .load(table),
        snapshot_id,
    )


def collect_hourly_environmental_report(
    spark: SparkSession,
    locations: tuple[OpenAQLocation, ...],
    weather_locations: tuple[WeatherLocation, ...],
    *,
    parameter: str = "pm25",
    hours: int = 48,
    end_at: datetime | None = None,
    now: datetime | None = None,
    air_snapshot_id: int | None = None,
    weather_snapshot_id: int | None = None,
) -> dict[str, Any]:
    generated_at = _utc(now or datetime.now(timezone.utc))
    start, end = report_period(hours, end_at, generated_at)
    station_rows = _station_rows(locations, weather_locations)
    if parameter not in AIR_PARAMETERS:
        raise ValueError(f"parameter must be one of {AIR_PARAMETERS}.")
    aggregates, air_snapshot = _read_snapshot(
        spark, ICEBERG_HOURLY_AGGREGATE_TABLE, air_snapshot_id
    )
    weather, weather_snapshot = _read_snapshot(
        spark, ICEBERG_WEATHER_CANONICAL_TABLE, weather_snapshot_id
    )
    rows = join_hourly_environment(
        aggregates,
        weather,
        locations,
        weather_locations,
        start=start,
        end=end,
        parameter=parameter,
    ).collect()
    if len(rows) != len(locations) * hours:
        raise ValueError(
            "Hourly aggregates are not unique for the selected station/hour."
        )
    by_location: dict[int, list[dict[str, Any]]] = {
        loc.location_id: [] for loc in locations
    }
    for row in rows:
        by_location[row["location_id"]].append(_hour_report(row.asDict(recursive=True)))
    series = []
    for identifier, name, city, weather_id in station_rows:
        points = by_location[identifier]
        series.append(
            {
                "location_id": identifier,
                "name": name,
                "city": city,
                "weather_location_id": weather_id,
                "coverage": {
                    "expected_hours": hours,
                    "air_quality_hours": sum(
                        p["air_quality"] is not None for p in points
                    ),
                    "complete_weather_hours": sum(
                        p["weather_status"] == "complete" for p in points
                    ),
                    "complete_join_hours": sum(
                        p["air_quality"] is not None
                        and p["weather_status"] == "complete"
                        for p in points
                    ),
                },
                "hours": points,
            }
        )
    return {
        "version": 1,
        "generated_at_utc": _iso(generated_at),
        "parameter": parameter,
        "unit": AIR_UNIT,
        "start_utc": _iso(start),
        "end_utc": _iso(end),
        "air_quality_basis": "finalized_hourly_statistics_of_ingested_observations",
        "weather_source": WEATHER_SOURCE,
        "weather_dataset": WEATHER_SOURCE_DATASET,
        "weather_alignment": {
            name: (
                "preceding_hour_sum_at_window_end"
                if name == "precipitation"
                else "point_at_window_start"
            )
            for name in sorted(WEATHER_PARAMETER_UNITS)
        },
        "inputs": {
            "air_quality": {
                "table": ICEBERG_HOURLY_AGGREGATE_TABLE,
                "snapshot_id": air_snapshot,
            },
            "weather": {
                "table": ICEBERG_WEATHER_CANONICAL_TABLE,
                "snapshot_id": weather_snapshot,
            },
        },
        "attribution": [
            {"name": "OpenAQ and upstream providers", "url": "https://openaq.org/"},
            {"name": "Open-Meteo", "url": "https://open-meteo.com/"},
        ],
        "locations": series,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Join finalized air-quality hours and latest canonical weather."
    )
    parser.add_argument("--parameter", choices=AIR_PARAMETERS, default="pm25")
    parser.add_argument("--hours", type=int, default=48)
    parser.add_argument("--end-at", type=datetime.fromisoformat)
    parser.add_argument("--locations-path", type=Path, default=DEFAULT_LOCATIONS_PATH)
    parser.add_argument(
        "--weather-locations-path", type=Path, default=DEFAULT_WEATHER_LOCATIONS_PATH
    )
    parser.add_argument("--air-snapshot-id", type=int)
    parser.add_argument("--weather-snapshot-id", type=int)
    args = parser.parse_args()
    try:
        report_period(args.hours, args.end_at, datetime.now(timezone.utc))
        for snapshot_id in (args.air_snapshot_id, args.weather_snapshot_id):
            if snapshot_id is not None and snapshot_id <= 0:
                raise ValueError("Snapshot IDs must be positive integers.")
    except ValueError as error:
        parser.error(str(error))
    locations = load_locations(args.locations_path)
    weather_locations = load_weather_locations(args.weather_locations_path)
    _station_rows(locations, weather_locations)
    spark = create_spark_session("hourly-environmental-insights")
    try:
        spark.sparkContext.setLogLevel("WARN")
        report = collect_hourly_environmental_report(
            spark,
            locations,
            weather_locations,
            parameter=args.parameter,
            hours=args.hours,
            end_at=args.end_at,
            air_snapshot_id=args.air_snapshot_id,
            weather_snapshot_id=args.weather_snapshot_id,
        )
    finally:
        spark.stop()
    print(
        json.dumps(
            report, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False
        )
    )


if __name__ == "__main__":
    main()
