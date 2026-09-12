import json
from typing import Any

from confluent_kafka import Consumer, Message


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "environment.measurements.raw"
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
) -> list[dict[str, Any]]:
    consumer = create_consumer(group_id)
    consumer.subscribe([KAFKA_TOPIC])

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
    events = consume_events()

    for event in events:
        print(json.dumps(event, ensure_ascii=False))

    print(f"Consumed {len(events)} events from {KAFKA_TOPIC}")


if __name__ == "__main__":
    main()
