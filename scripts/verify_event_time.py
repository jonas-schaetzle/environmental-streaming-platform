"""Exercise event-time behavior with an isolated local Kafka and Iceberg run."""

import json
from datetime import datetime, timedelta, timezone
from math import isclose
from pathlib import Path
from tempfile import mkdtemp

from confluent_kafka.admin import AdminClient, NewTopic
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, date_trunc, lit

from environmental_streaming.lakehouse.tables import (
    ICEBERG_CANONICAL_TABLE,
    ICEBERG_CATALOG,
    ICEBERG_HOURLY_AGGREGATE_TABLE,
    ensure_iceberg_tables,
)
from environmental_streaming.messaging.kafka_publisher import produce_events
from environmental_streaming.processing.measurement_model import (
    filter_valid_measurements,
    normalize_measurement_units,
)
from environmental_streaming.processing.measurement_stream import (
    aggregate_measurements,
    merge_iceberg_hourly_aggregate_batch,
    merge_iceberg_measurement_batch,
    parse_kafka_measurements,
    read_kafka_stream,
    write_iceberg_stream,
)
from environmental_streaming.runtime.spark import (
    ICEBERG_WAREHOUSE_PATH,
    create_spark_session,
)


ROOT = Path(__file__).resolve().parents[1]
BROKER = "127.0.0.1:19092"


def read_archived_measurement(spark: SparkSession) -> dict:
    archive = "spark.sql.catalog.archive_lake"
    spark.conf.set(archive, "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(f"{archive}.type", "hadoop")
    spark.conf.set(f"{archive}.warehouse", ICEBERG_WAREHOUSE_PATH)
    table = ICEBERG_CANONICAL_TABLE.replace(f"{ICEBERG_CATALOG}.", "archive_lake.", 1)
    row = (
        spark.table(table)
        .where((col("source") == "openaq") & (col("parameter") == "pm25"))
        .where(col("measured_at_utc") == date_trunc("hour", "measured_at_utc"))
        .where(col("raw_value").isNotNull())
        .orderBy(col("measured_at_utc").desc(), "location_id", "sensor_id")
        .select("raw_value")
        .first()
    )
    if row is None:
        raise RuntimeError(
            "The local warehouse needs an archived hourly OpenAQ PM2.5 row."
        )
    return json.loads(row["raw_value"])


def drain(spark: SparkSession, topic: str, run: Path) -> dict:
    messages = read_kafka_stream(spark, topic, BROKER)
    valid = filter_valid_measurements(
        normalize_measurement_units(parse_kafka_measurements(messages))
    )
    evidence = {}
    for name, frame, merge in (
        ("canonical", valid, merge_iceberg_measurement_batch),
        ("hourly", aggregate_measurements(valid), merge_iceberg_hourly_aggregate_batch),
    ):
        query = write_iceberg_stream(frame, merge, str(run / "checkpoints" / name))
        query.awaitTermination()
        evidence[name] = {
            "query_id": str(query.id),
            "run_id": str(query.runId),
            "input_rows": sum(batch["numInputRows"] for batch in query.recentProgress),
            "dropped_by_watermark": sum(
                operator["numRowsDroppedByWatermark"]
                for batch in query.recentProgress
                for operator in batch["stateOperators"]
            ),
        }
    return evidence


def stats(frame: DataFrame) -> dict | None:
    row = frame.select("measurement_count", "average_value").first()
    return row.asDict() if row is not None else None


def table(spark: SparkSession, name: str) -> DataFrame:
    spark.catalog.refreshTable(name)
    return spark.table(name)


def snapshots(spark: SparkSession) -> dict:
    result = {}
    for name in (ICEBERG_CANONICAL_TABLE, ICEBERG_HOURLY_AGGREGATE_TABLE):
        spark.catalog.refreshTable(name)
        ref = spark.table(f"{name}.refs").where(col("name") == "main").first()
        if ref is None:
            raise RuntimeError(f"{name} did not publish an Iceberg snapshot.")
        result[name] = ref.snapshot_id
    return result


def verify(spark: SparkSession, run: Path, topic: str) -> dict:
    seed = read_archived_measurement(spark)
    start = datetime.fromisoformat(seed["measured_at_utc"]).astimezone(timezone.utc)
    end = start + timedelta(hours=1)
    ensure_iceberg_tables(spark)
    admin = AdminClient({"bootstrap.servers": BROKER})
    admin.create_topics([NewTopic(topic, 1, 1)], request_timeout=15)[topic].result()

    def send(*events: dict) -> dict:
        produce_events(events, bootstrap_servers=BROKER, topic=topic)
        return drain(spark, topic, run)

    def target_hour() -> dict | None:
        hourly = table(spark, ICEBERG_HOURLY_AGGREGATE_TABLE)
        return stats(hourly.where(col("window_start") == lit(start)))

    def injected(delay: timedelta, value: float) -> dict:
        return {**seed, "measured_at_utc": (start + delay).isoformat(), "value": value}

    before = send(injected(timedelta(minutes=40), 20.0))
    assert target_hour() is None, "The first observation must leave the hour open."

    finalized = send(seed, injected(timedelta(hours=3, minutes=5), 10.0))
    published = target_hour()
    assert published is not None
    assert published["measurement_count"] == 2
    assert isclose(published["average_value"], (seed["value"] + 20.0) / 2)
    finalized_snapshots = snapshots(spark)

    delayed = send(injected(timedelta(minutes=30), 40.0))
    assert delayed["hourly"]["dropped_by_watermark"] == 1
    assert target_hour() == published
    after_late = snapshots(spark)
    assert (
        after_late[ICEBERG_CANONICAL_TABLE]
        != finalized_snapshots[ICEBERG_CANONICAL_TABLE]
    )
    assert (
        after_late[ICEBERG_HOURLY_AGGREGATE_TABLE]
        == (finalized_snapshots[ICEBERG_HOURLY_AGGREGATE_TABLE])
    )

    canonical = table(spark, ICEBERG_CANONICAL_TABLE)
    same_hour = canonical.where(
        (col("measured_at_utc") >= lit(start)) & (col("measured_at_utc") < lit(end))
    )
    late = same_hour.where(col("value") == 40.0).where(col("kafka_offset") == 3).first()
    assert late is not None and late.kafka_topic == topic and late.kafka_partition == 0
    recomputed = stats(aggregate_measurements(same_hour))
    assert recomputed is not None and recomputed["measurement_count"] == 3
    assert isclose(recomputed["average_value"], (seed["value"] + 60.0) / 3)

    restarted = drain(spark, topic, run)
    for sink, progress in restarted.items():
        assert progress["query_id"] == delayed[sink]["query_id"]
        assert progress["run_id"] != delayed[sink]["run_id"]
        assert progress["input_rows"] == 0
    assert snapshots(spark) == after_late
    assert target_hour() == published

    return {
        "window_start_utc": start.isoformat(),
        "window_end_utc": end.isoformat(),
        "station_id": seed["location_id"],
        "parameter": seed["parameter"],
        "archived_source_value": seed["value"],
        "late_record": {"topic": topic, "partition": 0, "offset": 3},
        "streamed_hour": published,
        "read_only_batch_recomputation": recomputed,
        "snapshots_before_late": finalized_snapshots,
        "snapshots_after_late_and_restart": after_late,
        "queries": {
            "open_window": before,
            "finalize_window": finalized,
            "late_input": delayed,
            "restart_without_new_input": restarted,
        },
    }


def main() -> None:
    verification = ROOT / "data" / "verification"
    verification.mkdir(parents=True, exist_ok=True)
    run = Path(mkdtemp(prefix="event_time_", dir=verification))
    topic = f"environment.verification.{run.name.replace('_', '-')}"
    spark = create_spark_session("event-time-verification")
    try:
        spark.conf.set(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", str(run / "warehouse")
        )
        spark.conf.set(f"spark.sql.catalog.{ICEBERG_CATALOG}.cache-enabled", "false")
        spark.sparkContext.setLogLevel("WARN")
        report = verify(spark, run, topic)
    finally:
        spark.stop()
    evidence = run / "report.json"
    evidence.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(evidence)


if __name__ == "__main__":
    main()
