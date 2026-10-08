import argparse
import json
from typing import Any

from confluent_kafka import Consumer, Message

from environmental_streaming.messaging.kafka_publisher import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
)

KAFKA_CONSUMER_GROUP = "environmental-streaming-debug-consumer"


def create_consumer(group_id: str = KAFKA_CONSUMER_GROUP) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


def decode_message(message: Message) -> dict[str, Any]:
    raw_key = message.key()
    raw_value = message.value()

    key = raw_key.decode("utf-8") if raw_key else None
    value = json.loads(raw_value.decode("utf-8"))

    return {
        "topic": message.topic(),
        "partition": message.partition(),
        "offset": message.offset(),
        "key": key,
        "value": value,
    }


def consume_events(
    max_messages: int = 10,
    timeout_seconds: float = 5.0,
    group_id: str = KAFKA_CONSUMER_GROUP,
    topic: str = KAFKA_TOPIC,
) -> list[dict[str, Any]]:
    consumer = create_consumer(group_id)
    consumer.subscribe([topic])

    events = []

    try:
        while len(events) < max_messages:
            message = consumer.poll(timeout_seconds)

            if message is None:
                break

            if message.error():
                raise RuntimeError(message.error())

            events.append(decode_message(message))
    finally:
        consumer.close()

    return events


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect Kafka events without committing offsets."
    )
    parser.add_argument("--topic", default=KAFKA_TOPIC)
    parser.add_argument("--max-messages", type=int, default=10)
    parser.add_argument("--timeout-seconds", type=float, default=5.0)
    parser.add_argument("--group-id", default=KAFKA_CONSUMER_GROUP)
    args = parser.parse_args()
    events = consume_events(
        max_messages=args.max_messages,
        timeout_seconds=args.timeout_seconds,
        group_id=args.group_id,
        topic=args.topic,
    )

    for event in events:
        print(json.dumps(event, ensure_ascii=False))

    print(f"Consumed {len(events)} events from {args.topic}")


if __name__ == "__main__":
    main()
