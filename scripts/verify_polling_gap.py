"""Verify bounded weather catch-up after a simulated gap using live source data."""

import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from tempfile import mkdtemp
from unittest.mock import patch

from confluent_kafka.admin import AdminClient, NewTopic
from pyspark.sql import SparkSession

from environmental_streaming.ingestion import weather_producer
from environmental_streaming.ingestion.weather_client import (
    HourlyWeatherResponse,
    fetch_hourly_weather,
)
from environmental_streaming.ingestion.weather_events import (
    build_canonical_weather_events,
)
from environmental_streaming.ingestion.weather_locations import (
    DEFAULT_WEATHER_LOCATIONS_PATH,
    load_weather_locations,
)
from environmental_streaming.lakehouse.tables import (
    ICEBERG_CATALOG,
    ICEBERG_WEATHER_CANONICAL_TABLE,
    ICEBERG_WEATHER_QUARANTINE_TABLE,
    ensure_weather_canonical_table,
    ensure_weather_quarantine_table,
)
from environmental_streaming.messaging.kafka_publisher import WEATHER_KAFKA_TOPIC
from environmental_streaming.processing.measurement_stream import read_kafka_stream
from environmental_streaming.processing.weather_model import (
    WEATHER_PARAMETER_UNITS,
    filter_invalid_weather_observations,
    filter_valid_weather_observations,
)
from environmental_streaming.processing.weather_stream import (
    parse_kafka_weather,
    write_weather_canonical_stream,
    write_weather_quarantine_stream,
)
from environmental_streaming.runtime.spark import create_spark_session
from environmental_streaming.runtime.timestamps import collect_utc_rows


ROOT = Path(__file__).resolve().parents[1]
BROKER = "127.0.0.1:19092"
PAST_HOURS = 6
TABLE = ICEBERG_WEATHER_CANONICAL_TABLE


def rows(spark: SparkSession) -> dict:
    spark.catalog.refreshTable(TABLE)
    records = collect_utc_rows(
        spark.table(TABLE).select(
            "weather_location_id",
            "parameter",
            "measured_at_utc",
            "fetched_at_utc",
            "value",
            "unit",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
        ),
        ("measured_at_utc", "fetched_at_utc"),
    )
    indexed = {
        (
            row["weather_location_id"],
            row["parameter"],
            row["measured_at_utc"].isoformat(),
        ): row
        for row in records
    }
    assert len(indexed) == len(records), "Duplicate canonical weather keys."
    return indexed


def drain(spark: SparkSession, run: Path) -> None:
    parsed = parse_kafka_weather(read_kafka_stream(spark, WEATHER_KAFKA_TOPIC, BROKER))
    write_weather_canonical_stream(
        filter_valid_weather_observations(parsed),
        str(run / "canonical-checkpoint"),
    ).awaitTermination()
    write_weather_quarantine_stream(
        filter_invalid_weather_observations(parsed),
        str(run / "quarantine-checkpoint"),
    ).awaitTermination()


def verify(spark: SparkSession, run: Path) -> dict:
    admin = AdminClient({"bootstrap.servers": BROKER})
    existing = admin.list_topics(timeout=15).topics
    if WEATHER_KAFKA_TOPIC in existing:
        raise RuntimeError(
            "Use a fresh isolated broker with no canonical weather topic."
        )
    admin.create_topics([NewTopic(WEATHER_KAFKA_TOPIC, 1, 1)], request_timeout=15)[
        WEATHER_KAFKA_TOPIC
    ].result()
    ensure_weather_canonical_table(spark)
    ensure_weather_quarantine_table(spark)
    locations = load_weather_locations(ROOT / DEFAULT_WEATHER_LOCATIONS_PATH)
    state_path = run / "producer-state.json"
    before_source = {}
    after_source = {}
    baseline_reports = []

    for location in locations:
        response = fetch_hourly_weather(location, past_hours=PAST_HOURS)
        before_source[location.weather_location_id] = {
            "location": asdict(location),
            "fetched_at_utc": response.fetched_at_utc.isoformat(),
            "response": response.payload,
        }
        # Publish only the oldest observed hour to stage missed subsequent polls.
        payload = {
            **response.payload,
            "hourly": {
                key: values[:1] for key, values in response.payload["hourly"].items()
            },
        }
        baseline = HourlyWeatherResponse(payload, response.fetched_at_utc)
        with patch.object(
            weather_producer, "fetch_hourly_weather", return_value=baseline
        ):
            baseline_reports.append(
                weather_producer.publish_weather_location(
                    location,
                    past_hours=PAST_HOURS,
                    bootstrap_servers=BROKER,
                    state_path=state_path,
                )
            )
    drain(spark, run)
    before = rows(spark)
    assert len(before) == len(locations) * len(WEATHER_PARAMETER_UNITS)

    def capture_response(location, past_hours):
        response = fetch_hourly_weather(location, past_hours=past_hours)
        after_source[location.weather_location_id] = {
            "location": asdict(location),
            "fetched_at_utc": response.fetched_at_utc.isoformat(),
            "response": response.payload,
        }
        return response

    with patch.object(
        weather_producer, "fetch_hourly_weather", side_effect=capture_response
    ):
        resumed = weather_producer.publish_weather_locations(
            locations,
            past_hours=PAST_HOURS,
            bootstrap_servers=BROKER,
            state_path=state_path,
        )
    assert resumed["locations_failed"] == 0, resumed["errors"]
    drain(spark, run)
    after = rows(spark)
    coverage = []
    for location in locations:
        identifier = location.weather_location_id
        captured = after_source[identifier]
        response = HourlyWeatherResponse(
            captured["response"],
            datetime.fromisoformat(captured["fetched_at_utc"]),
        )
        events = build_canonical_weather_events(location, response)
        expected = {
            (identifier, event["parameter"], event["measured_at_utc"]): event
            for event in events
        }
        assert all(key in after for key in expected), "Catch-up missed a returned key."
        for key, event in expected.items():
            assert after[key]["value"] == event["value"]
            assert after[key]["unit"] == event["unit"]
        added = [after[key] for key in expected if key not in before]
        assert added, "The source returned no missing hours to recover."
        assert all(row["kafka_topic"] == WEATHER_KAFKA_TOPIC for row in added)
        coverage.append(
            {
                "weather_location_id": identifier,
                "hours_returned_on_resume": len({key[2] for key in expected}),
                "previously_absent_hours_recovered": sorted(
                    {row["measured_at_utc"].isoformat() for row in added}
                ),
                "new_canonical_records": len(added),
                "example_kafka_identity": {
                    key: added[0][key]
                    for key in ("kafka_topic", "kafka_partition", "kafka_offset")
                },
            }
        )
    quarantine_count = spark.table(ICEBERG_WEATHER_QUARANTINE_TABLE).count()
    assert quarantine_count == 0
    for label, captures in (
        ("baseline-source", before_source),
        ("resumed-source", after_source),
    ):
        (run / f"{label}.json").write_text(json.dumps(captures, indent=2) + "\n")
    return {
        "scenario": "simulated missed polls followed by live Open-Meteo catch-up",
        "requested_past_hours": PAST_HOURS,
        "baseline_reports": baseline_reports,
        "resumed_report": resumed,
        "canonical_records_before": len(before),
        "canonical_records_after": len(after),
        "quarantine_records": quarantine_count,
        "coverage": coverage,
    }


def main() -> None:
    directory = ROOT / "data" / "verification"
    directory.mkdir(parents=True, exist_ok=True)
    run = Path(mkdtemp(prefix="polling_gap_", dir=directory))
    spark = create_spark_session("polling-gap-verification")
    try:
        spark.conf.set(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", str(run / "warehouse")
        )
        spark.conf.set(f"spark.sql.catalog.{ICEBERG_CATALOG}.cache-enabled", "false")
        spark.sparkContext.setLogLevel("WARN")
        report = verify(spark, run)
    finally:
        spark.stop()
    evidence = run / "report.json"
    evidence.write_text(json.dumps(report, indent=2) + "\n")
    print(evidence)


if __name__ == "__main__":
    main()
