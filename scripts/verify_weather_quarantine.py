"""Trace a weather data-quality alert to its quarantined Kafka record."""

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import mkdtemp

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from pyspark.sql import SparkSession

from environmental_streaming.diagnostics.kafka_consumer import decode_message
from environmental_streaming.ingestion.openaq_producer import (
    DEFAULT_LOCATIONS_PATH,
    load_locations,
)
from environmental_streaming.ingestion.weather_locations import (
    DEFAULT_WEATHER_LOCATIONS_PATH,
    load_weather_locations,
)
from environmental_streaming.ingestion.weather_replay import load_weather_events
from environmental_streaming.lakehouse.tables import (
    ICEBERG_CATALOG,
    ICEBERG_WEATHER_CANONICAL_TABLE,
    ICEBERG_WEATHER_QUARANTINE_TABLE,
    ensure_iceberg_tables,
    ensure_weather_canonical_table,
    ensure_weather_quarantine_table,
)
from environmental_streaming.messaging.kafka_publisher import (
    kafka_key_for_weather_event,
    produce_events,
)
from environmental_streaming.processing.measurement_stream import read_kafka_stream
from environmental_streaming.processing.weather_model import (
    filter_invalid_weather_observations,
    filter_valid_weather_observations,
)
from environmental_streaming.processing.weather_stream import (
    parse_kafka_weather,
    write_weather_canonical_stream,
    write_weather_quarantine_stream,
)
from environmental_streaming.products.air_quality_insights import (
    DEFAULT_FRESHNESS_THRESHOLD_MINUTES,
    collect_air_quality_report,
)
from environmental_streaming.products.data_quality_alerts import (
    QUARANTINE_TABLES,
    build_alert_report,
    summarize_quarantine,
)
from environmental_streaming.products.hourly_environmental_insights import (
    collect_hourly_environmental_report,
)
from environmental_streaming.runtime.spark import create_spark_session
from environmental_streaming.runtime.timestamps import collect_utc_rows


ROOT = Path(__file__).resolve().parents[1]
BROKER = "127.0.0.1:19092"


def collect_alert(spark: SparkSession) -> dict:
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=24)
    locations = load_locations(ROOT / DEFAULT_LOCATIONS_PATH)
    weather_locations = load_weather_locations(ROOT / DEFAULT_WEATHER_LOCATIONS_PATH)
    hourly = collect_hourly_environmental_report(
        spark,
        locations,
        weather_locations,
        now=now,
    )
    latest = collect_air_quality_report(
        spark,
        locations,
        now=now,
        freshness_threshold_seconds=DEFAULT_FRESHNESS_THRESHOLD_MINUTES * 60,
    )
    quarantine = {
        table: summarize_quarantine(spark.table(table), start, now)
        for table in QUARANTINE_TABLES
    }
    report = build_alert_report(
        latest,
        hourly,
        quarantine,
        quarantine_start=start,
        now=now,
        minimum_coverage_percent=50,
    )
    alerts = [
        alert for alert in report["alerts"] if alert["code"] == "recent_quarantine"
    ]
    assert len(alerts) == 1, f"Expected one quarantine alert, got {len(alerts)}."
    return alerts[0]


def read_alert_record(identity: dict) -> tuple[dict, bytes, datetime]:
    consumer = Consumer(
        {
            "bootstrap.servers": BROKER,
            "group.id": f"{identity['topic']}-trace",
            "enable.auto.commit": False,
        }
    )
    try:
        consumer.assign(
            [
                TopicPartition(
                    identity["topic"],
                    identity["partition"],
                    identity["offset"],
                )
            ]
        )
        message = consumer.poll(15)
        assert message is not None, "Kafka lookup timed out."
        assert not message.error(), message.error()
        timestamp = datetime.fromtimestamp(message.timestamp()[1] / 1000, timezone.utc)
        return decode_message(message), message.value(), timestamp
    finally:
        consumer.close()


def verify(spark: SparkSession, injected: dict, run: Path) -> dict:
    topic = f"environment.verification.{run.name.replace('_', '-')}"
    admin = AdminClient({"bootstrap.servers": BROKER})
    admin.create_topics([NewTopic(topic, 1, 1)], request_timeout=15)[topic].result()
    ensure_iceberg_tables(spark)
    ensure_weather_canonical_table(spark)
    ensure_weather_quarantine_table(spark)

    published = produce_events(
        [injected],
        bootstrap_servers=BROKER,
        topic=topic,
        key_for_event=kafka_key_for_weather_event,
    )
    assert published == 1
    parsed = parse_kafka_weather(read_kafka_stream(spark, topic, BROKER))
    canonical = write_weather_canonical_stream(
        filter_valid_weather_observations(parsed),
        str(run / "canonical-checkpoint"),
    )
    canonical.awaitTermination()
    quarantine = write_weather_quarantine_stream(
        filter_invalid_weather_observations(parsed),
        str(run / "quarantine-checkpoint"),
    )
    quarantine.awaitTermination()
    processed_end_offsets = {}
    for sink, query in (("canonical", canonical), ("quarantine", quarantine)):
        progress = query.lastProgress
        assert progress is not None, f"No Spark progress for the {sink} sink."
        end_offset = json.loads(progress.json)["sources"][0]["endOffset"]
        assert end_offset == {topic: {"0": 1}}, f"Unexpected {sink} Kafka end offset."
        processed_end_offsets[sink] = end_offset
    canonical_count = spark.table(ICEBERG_WEATHER_CANONICAL_TABLE).count()
    assert canonical_count == 0

    alert = collect_alert(spark)
    evidence = alert["evidence"]
    assert evidence["table"] == ICEBERG_WEATHER_QUARANTINE_TABLE
    assert evidence["row_count"] == 1
    (reason,) = evidence["reasons"]
    assert reason["validation_error"] == "missing_value" and reason["row_count"] == 1
    identity = reason["latest_kafka_record"]
    assert (identity["topic"], identity["partition"], identity["offset"]) == (
        topic,
        0,
        0,
    )

    rows = collect_utc_rows(
        spark.table(evidence["table"]).select(
            "validation_error",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "kafka_key",
            "raw_value",
        ),
        ("kafka_timestamp",),
    )
    (row,) = rows
    record, payload, timestamp = read_alert_record(identity)
    for key in ("topic", "partition", "offset", "key"):
        assert row[f"kafka_{key}"] == record[key]
    assert (
        row["kafka_timestamp"]
        == timestamp
        == datetime.fromisoformat(identity["timestamp_utc"])
    )
    assert row["validation_error"] == reason["validation_error"]
    assert row["raw_value"].encode("utf-8") == payload
    assert record["value"] == injected
    return {
        "canonical_row_count": canonical_count,
        "processed_end_offsets": processed_end_offsets,
        "alert": alert,
        "quarantine_row": row,
        "kafka_record": record,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "events_file", type=Path, help="Archived canonical weather JSONL"
    )
    args = parser.parse_args()
    original = load_weather_events(args.events_file)[0]
    if original.get("value") is None:
        parser.error("The first archived weather record must contain a value.")

    verification = ROOT / "data" / "verification"
    verification.mkdir(parents=True, exist_ok=True)
    run = Path(mkdtemp(prefix="weather_quarantine_", dir=verification))
    spark = create_spark_session("weather-quarantine-verification")
    try:
        spark.conf.set(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", str(run / "warehouse")
        )
        spark.conf.set(f"spark.sql.catalog.{ICEBERG_CATALOG}.cache-enabled", "false")
        spark.sparkContext.setLogLevel("WARN")
        report = verify(spark, {**original, "value": None}, run)
    finally:
        spark.stop()
    evidence = run / "report.json"
    evidence.write_text(
        json.dumps(report, indent=2, default=datetime.isoformat) + "\n",
        encoding="utf-8",
    )
    print(evidence)


if __name__ == "__main__":
    main()
