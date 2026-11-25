"""Verify Linux process restarts using a copy of retained real pipeline state."""

import hashlib
import json
import subprocess
import sys
import time
import uuid
from pathlib import Path

from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient

from environmental_streaming.lakehouse.tables import ICEBERG_WEATHER_CANONICAL_TABLE
from environmental_streaming.messaging.kafka_publisher import (
    KAFKA_TOPIC,
    WEATHER_KAFKA_TOPIC,
    produce_weather_events,
)
from environmental_streaming.processing.measurement_model import (
    filter_invalid_measurements,
    filter_valid_measurements,
    normalize_measurement_units,
)
from environmental_streaming.processing.measurement_stream import (
    aggregate_measurements,
    parse_kafka_measurements,
    read_kafka_stream,
    write_iceberg_hourly_aggregate_stream,
    write_iceberg_measurement_stream,
    write_iceberg_quarantine_stream,
)
from environmental_streaming.processing.weather_model import (
    filter_invalid_weather_observations,
    filter_valid_weather_observations,
)
from environmental_streaming.processing.weather_stream import (
    parse_kafka_weather,
    write_weather_canonical_stream,
    write_weather_quarantine_stream,
)
from environmental_streaming.runtime.spark import create_spark_session

from runtime_sizing import DATA, KAFKA


CHECKPOINTS = {
    "air-canonical": "iceberg_canonical_measurements",
    "air-quarantine": "iceberg_quarantine_measurements",
    "air-hourly": "iceberg_hourly_measurement_aggregates",
    "weather-canonical": "weather_canonical",
    "weather-quarantine": "weather_quarantine",
}
TABLES = (
    "canonical_measurements",
    "quarantine_measurements",
    "hourly_measurement_aggregates",
    "canonical_weather_observations",
    "quarantine_weather_observations",
)


def hashes(directory: Path) -> dict:
    return {
        str(path.relative_to(directory)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(directory.rglob("*"))
        if path.is_file()
    }


def checkpoints() -> dict:
    result = {}
    for name, folder in CHECKPOINTS.items():
        directory = DATA / "checkpoints" / folder
        batch = max(
            int(path.name)
            for path in (directory / "commits").iterdir()
            if path.name.isdigit()
        )
        result[name] = {
            "query_id": json.loads((directory / "metadata").read_text())["id"],
            "committed_batch": batch,
            "offsets": json.loads(
                (directory / "offsets" / str(batch)).read_text().splitlines()[-1]
            ),
        }
    return result


def spark_phase(name: str) -> None:
    spark = create_spark_session(f"linux-restart-{name}")
    spark.sparkContext.setLogLevel("WARN")
    progress = {}
    try:
        if name != "baseline":
            parsed = parse_kafka_measurements(read_kafka_stream(spark))
            valid = filter_valid_measurements(normalize_measurement_units(parsed))
            weather = parse_kafka_weather(read_kafka_stream(spark, WEATHER_KAFKA_TOPIC))
            for query_name, frame, writer in (
                ("air-canonical", valid, write_iceberg_measurement_stream),
                (
                    "air-quarantine",
                    filter_invalid_measurements(parsed),
                    write_iceberg_quarantine_stream,
                ),
                (
                    "air-hourly",
                    aggregate_measurements(valid),
                    write_iceberg_hourly_aggregate_stream,
                ),
                (
                    "weather-canonical",
                    filter_valid_weather_observations(weather),
                    write_weather_canonical_stream,
                ),
                (
                    "weather-quarantine",
                    filter_invalid_weather_observations(weather),
                    write_weather_quarantine_stream,
                ),
            ):
                query = writer(frame)
                query.awaitTermination()
                progress[query_name] = {
                    "query_id": str(query.id),
                    "run_id": str(query.runId),
                    "input_rows": sum(
                        batch["numInputRows"] for batch in query.recentProgress
                    ),
                }
        tables = {}
        for name_part in TABLES:
            table = f"local.lake.{name_part}"
            spark.catalog.refreshTable(table)
            records = sorted(spark.table(table).toJSON().collect())
            refs = spark.table(f"{table}.refs").where("name = 'main'").collect()
            tables[name_part] = {
                "row_count": len(records),
                "rows_sha256": hashlib.sha256(json.dumps(records).encode()).hexdigest(),
                "snapshot_id": refs[0].snapshot_id if refs else None,
            }
        seed = (
            spark.table(ICEBERG_WEATHER_CANONICAL_TABLE)
            .orderBy("weather_location_id", "parameter", "measured_at_utc")
            .select("raw_value")
            .first()
        )
        report = {
            "spark_application_id": spark.sparkContext.applicationId,
            "tables": tables,
            "queries": progress,
            "checkpoints": checkpoints(),
            "weather_event": json.loads(seed.raw_value),
        }
        (DATA / f"{name}.json").write_text(json.dumps(report, indent=2) + "\n")
    finally:
        spark.stop()


def kafka_inventory() -> dict:
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": f"restart-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )
    result = {}
    try:
        metadata = consumer.list_topics(timeout=15)
        for topic in (KAFKA_TOPIC, WEATHER_KAFKA_TOPIC):
            result[topic] = {}
            for partition in sorted(metadata.topics[topic].partitions):
                tp = TopicPartition(topic, partition)
                low, high = consumer.get_watermark_offsets(tp, timeout=15)
                consumer.assign([TopicPartition(topic, partition, low)])
                records = []
                deadline = time.monotonic() + 30
                while len(records) < high - low:
                    message = consumer.poll(1)
                    if message is not None:
                        if message.error():
                            raise KafkaException(message.error())
                        records.append(
                            [
                                message.offset(),
                                message.key().hex(),
                                message.value().hex(),
                                message.timestamp(),
                            ]
                        )
                    if time.monotonic() > deadline:
                        raise TimeoutError(
                            "Could not read the retained partition range."
                        )
                topic_file = (
                    DATA / "kafka" / f"{topic}-{partition}" / "partition.metadata"
                )
                result[topic][str(partition)] = {
                    "low": low,
                    "high": high,
                    "topic_metadata": topic_file.read_text(),
                    "records_sha256": hashlib.sha256(
                        json.dumps(records).encode()
                    ).hexdigest(),
                }
    finally:
        consumer.close()
    cluster = next(
        line
        for line in (DATA / "kafka" / "meta.properties").read_text().splitlines()
        if line.startswith("cluster.id=")
    )
    return {"cluster": cluster, "topics": result}


def main() -> None:
    original_state = hashes(DATA / "state")
    original_checkpoints = hashes(DATA / "checkpoints")
    broker = None

    def start(label: str):
        with (DATA / f"broker-{label}.log").open("w") as log:
            process = subprocess.Popen(
                [str(KAFKA / "kafka-server-start.sh"), str(DATA / "broker.properties")],
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        admin = AdminClient({"bootstrap.servers": "localhost:9092"})
        for _ in range(60):
            assert process.poll() is None, "Broker exited; inspect its log."
            try:
                admin.list_topics(timeout=1)
                return process
            except KafkaException:
                time.sleep(1)
        process.terminate()
        process.wait(timeout=60)
        raise TimeoutError("Broker readiness failed.")

    def stop() -> None:
        broker.terminate()
        assert broker.wait(timeout=60) in (0, 143, -15)

    def phase(name: str) -> dict:
        print(f"Starting {name}", flush=True)
        with (
            (DATA / f"{name}.stdout").open("w") as stdout,
            (DATA / f"{name}.stderr").open("w") as stderr,
        ):
            subprocess.run(
                [sys.executable, __file__, "--phase", name],
                check=True,
                stdout=stdout,
                stderr=stderr,
                timeout=600,
            )
        return json.loads((DATA / f"{name}.json").read_text())

    try:
        broker = start("baseline")
        before_kafka = kafka_inventory()
        baseline = phase("baseline")
        stop()
        broker = start("no-input")
        assert kafka_inventory() == before_kafka
        no_input = phase("no-input")
        assert no_input["tables"] == baseline["tables"]
        assert all(query["input_rows"] == 0 for query in no_input["queries"].values())
        for name, query in no_input["queries"].items():
            assert query["query_id"] == baseline["checkpoints"][name]["query_id"]
            assert (
                no_input["checkpoints"][name]["offsets"]
                == baseline["checkpoints"][name]["offsets"]
            )
        assert produce_weather_events([baseline["weather_event"]]) == 1
        pending_kafka = kafka_inventory()
        stop()
        broker = start("pending-input")
        assert kafka_inventory() == pending_kafka
        resumed = phase("resumed")
        for table, row in resumed["tables"].items():
            assert row["row_count"] == baseline["tables"][table]["row_count"]
            assert row["rows_sha256"] == baseline["tables"][table]["rows_sha256"]
        for name, query in resumed["queries"].items():
            assert query["query_id"] == no_input["queries"][name]["query_id"]
            assert query["run_id"] != no_input["queries"][name]["run_id"]
            assert query["input_rows"] == (1 if name.startswith("weather-") else 0)
            topic = WEATHER_KAFKA_TOPIC if name.startswith("weather-") else KAFKA_TOPIC
            assert resumed["checkpoints"][name]["offsets"][topic] == {
                partition: data["high"]
                for partition, data in pending_kafka["topics"][topic].items()
            }
        assert hashes(DATA / "state") == original_state
        final_checkpoints = hashes(DATA / "checkpoints")
        assert all(
            final_checkpoints.get(name) == digest
            for name, digest in original_checkpoints.items()
        )
        report = {
            "scope": "Linux broker and Spark process restarts; copied retained live-source state",
            "baseline": baseline,
            "no_input": no_input,
            "resumed": resumed,
            "kafka_before": before_kafka,
            "kafka_with_pending_replay": pending_kafka,
            "producer_state_files_unchanged": len(original_state),
            "original_checkpoint_files_preserved": len(original_checkpoints),
            "host_reboot_verified": False,
        }
        (DATA / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    finally:
        if broker is not None and broker.poll() is None:
            stop()


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--phase":
        spark_phase(sys.argv[2])
    else:
        main()
