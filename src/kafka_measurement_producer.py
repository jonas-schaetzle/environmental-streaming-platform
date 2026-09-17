import argparse
import json
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from confluent_kafka import Producer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "environment.measurements.raw"
SAMPLE_EVENTS_PATH = Path("data/stream/input/canonical_measurements_001.jsonl")


@dataclass(frozen=True)
class ProducerConfig:
    input_path: Path = SAMPLE_EVENTS_PATH
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS
    topic: str = KAFKA_TOPIC


def read_jsonl(path: Path) -> list[dict]:
    events = []

    with path.open(encoding="utf-8") as file:
        for line in file:
            stripped_line = line.strip()

            if stripped_line:
                events.append(json.loads(stripped_line))

    return events


def kafka_key_for_event(event: dict) -> str:
    return str(event["sensor_id"])


def produce_events(
    events: Iterable[dict],
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    topic: str = KAFKA_TOPIC,
) -> int:
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    produced_count = 0

    for event in events:
        key = kafka_key_for_event(event)
        value = json.dumps(event, ensure_ascii=False, separators=(",", ":"))

        producer.produce(
            topic=topic,
            key=key,
            value=value,
        )
        produced_count += 1

    undelivered_count = producer.flush()
    if undelivered_count:
        raise RuntimeError(
            f"Kafka producer could not deliver {undelivered_count} events."
        )

    return produced_count


def parse_args() -> ProducerConfig:
    parser = argparse.ArgumentParser(
        description="Produce canonical measurement JSONL events to Kafka."
    )
    parser.add_argument(
        "--input-path",
        type=Path,
        default=SAMPLE_EVENTS_PATH,
        help="Path to a JSONL file containing canonical measurement events.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=KAFKA_BOOTSTRAP_SERVERS,
        help="Kafka bootstrap servers.",
    )
    parser.add_argument(
        "--topic",
        default=KAFKA_TOPIC,
        help="Kafka topic to produce to.",
    )

    args = parser.parse_args()

    return ProducerConfig(
        input_path=args.input_path,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
    )


def main() -> None:
    config = parse_args()
    events = read_jsonl(config.input_path)
    produced_count = produce_events(
        events,
        bootstrap_servers=config.bootstrap_servers,
        topic=config.topic,
    )
    print(f"Produced {produced_count} events to {config.topic}")


if __name__ == "__main__":
    main()
