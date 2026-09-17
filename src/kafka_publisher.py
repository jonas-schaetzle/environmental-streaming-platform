import json
from collections.abc import Iterable
from typing import Any

from confluent_kafka import Producer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "environment.measurements.canonical"


def kafka_key_for_event(event: dict[str, Any]) -> str:
    return str(event["sensor_id"])


def produce_events(
    events: Iterable[dict[str, Any]],
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    topic: str = KAFKA_TOPIC,
) -> int:
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    produced_count = 0

    for event in events:
        producer.produce(
            topic=topic,
            key=kafka_key_for_event(event),
            value=json.dumps(event, ensure_ascii=False, separators=(",", ":")),
        )
        produced_count += 1

    undelivered_count = producer.flush()
    if undelivered_count:
        raise RuntimeError(
            f"Kafka producer could not deliver {undelivered_count} events."
        )

    return produced_count
