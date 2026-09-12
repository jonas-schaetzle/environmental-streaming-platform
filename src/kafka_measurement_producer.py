import json
from collections.abc import Iterable
from pathlib import Path

from confluent_kafka import Producer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "environment.measurements.raw"
SAMPLE_EVENTS_PATH = Path("data/stream/input/canonical_measurements_001.jsonl")


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


def produce_events(events: Iterable[dict]) -> int:
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    produced_count = 0

    for event in events:
        key = kafka_key_for_event(event)
        value = json.dumps(event, ensure_ascii=False, separators=(",", ":"))

        producer.produce(
            topic=KAFKA_TOPIC,
            key=key,
            value=value,
        )
        produced_count += 1

    producer.flush()
    return produced_count


def main() -> None:
    events = read_jsonl(SAMPLE_EVENTS_PATH)
    produced_count = produce_events(events)
    print(f"Produced {produced_count} events to {KAFKA_TOPIC}")


if __name__ == "__main__":
    main()
