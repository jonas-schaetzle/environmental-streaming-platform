"""Verify that a newer weather receipt updates one canonical Iceberg row."""

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import mkdtemp

from confluent_kafka.admin import AdminClient, NewTopic
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from environmental_streaming.ingestion.weather_replay import load_weather_events
from environmental_streaming.lakehouse.tables import (
    ICEBERG_CATALOG,
    ICEBERG_WEATHER_CANONICAL_TABLE,
    ensure_weather_canonical_table,
)
from environmental_streaming.messaging.kafka_publisher import (
    kafka_key_for_weather_event,
    produce_events,
)
from environmental_streaming.processing.measurement_stream import read_kafka_stream
from environmental_streaming.processing.weather_model import (
    WEATHER_IDENTITY_COLUMNS,
    filter_valid_weather_observations,
)
from environmental_streaming.processing.weather_stream import (
    parse_kafka_weather,
    write_weather_canonical_stream,
)
from environmental_streaming.runtime.spark import create_spark_session
from environmental_streaming.runtime.timestamps import collect_utc_rows


ROOT = Path(__file__).resolve().parents[1]
BROKER = "127.0.0.1:19092"
TABLE = ICEBERG_WEATHER_CANONICAL_TABLE
RECORD_FIELDS = (
    *WEATHER_IDENTITY_COLUMNS,
    "value",
    "unit",
    "fetched_at_utc",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
)


def inspect_table(spark: SparkSession) -> dict:
    spark.catalog.refreshTable(TABLE)
    rows = collect_utc_rows(
        spark.table(TABLE).select(*RECORD_FIELDS),
        ("measured_at_utc", "fetched_at_utc"),
    )
    assert len(rows) == 1, f"Expected one canonical row, found {len(rows)}."
    ref = spark.table(f"{TABLE}.refs").where(col("name") == "main").first()
    assert ref is not None, "Expected an Iceberg snapshot on main."
    return {"row_count": len(rows), "snapshot_id": ref.snapshot_id, "record": rows[0]}


def verify(spark: SparkSession, original: dict, run: Path) -> dict:
    topic = f"environment.verification.{run.name.replace('_', '-')}"
    admin = AdminClient({"bootstrap.servers": BROKER})
    admin.create_topics([NewTopic(topic, 1, 1)], request_timeout=15)[topic].result()
    ensure_weather_canonical_table(spark)
    messages = read_kafka_stream(spark, topic, BROKER)
    valid = filter_valid_weather_observations(parse_kafka_weather(messages))
    checkpoint = str(run / "checkpoint")

    def send_and_process(event: dict) -> dict:
        published = produce_events(
            [event],
            bootstrap_servers=BROKER,
            topic=topic,
            key_for_event=kafka_key_for_weather_event,
        )
        assert published == 1
        query = write_weather_canonical_stream(valid, checkpoint)
        query.awaitTermination()
        input_rows = sum(batch["numInputRows"] for batch in query.recentProgress)
        assert input_rows == 1, f"Expected one Kafka input row, consumed {input_rows}."
        return {
            **inspect_table(spark),
            "query_id": str(query.id),
            "run_id": str(query.runId),
            "input_rows": input_rows,
        }

    receipt = datetime.fromisoformat(original["fetched_at_utc"])
    correction = {
        **original,
        "value": original["value"] + 2.0,
        "fetched_at_utc": (receipt + timedelta(minutes=2)).isoformat(),
    }
    before = send_and_process(original)
    after = send_and_process(correction)
    for offset, event, step in ((0, original, before), (1, correction, after)):
        record = step["record"]
        for field in WEATHER_IDENTITY_COLUMNS:
            expected = event[field]
            if field == "measured_at_utc":
                expected = datetime.fromisoformat(expected)
            assert record[field] == expected, f"Unexpected {field} at offset {offset}."
        assert record["value"] == event["value"]
        assert record["unit"] == event["unit"]
        assert record["fetched_at_utc"] == datetime.fromisoformat(
            event["fetched_at_utc"]
        )
        assert (
            record["kafka_topic"],
            record["kafka_partition"],
            record["kafka_offset"],
        ) == (
            topic,
            0,
            offset,
        )
    assert before["snapshot_id"] != after["snapshot_id"]
    assert before["query_id"] == after["query_id"]
    assert before["run_id"] != after["run_id"]
    return {
        "source": "archived Open-Meteo record with a simulated correction",
        "before": before,
        "after": after,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "events_file", type=Path, help="Archived canonical weather JSONL"
    )
    args = parser.parse_args()
    original = next(
        (
            event
            for event in load_weather_events(args.events_file)
            if event.get("parameter") == "temperature_2m"
            and event.get("value") is not None
        ),
        None,
    )
    if original is None:
        parser.error("Provide a canonical weather file containing a temperature value.")

    verification = ROOT / "data" / "verification"
    verification.mkdir(parents=True, exist_ok=True)
    run = Path(mkdtemp(prefix="weather_correction_", dir=verification))
    spark = create_spark_session("weather-correction-verification")
    try:
        spark.conf.set(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", str(run / "warehouse")
        )
        spark.conf.set(f"spark.sql.catalog.{ICEBERG_CATALOG}.cache-enabled", "false")
        spark.sparkContext.setLogLevel("WARN")
        report = verify(spark, original, run)
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
