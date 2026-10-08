import json
from collections.abc import Callable, Iterable
from typing import Any

from confluent_kafka import Producer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "environment.measurements.canonical"
WEATHER_KAFKA_TOPIC = "environment.weather.canonical"


def kafka_key_for_event(event: dict[str, Any]) -> str:
    return str(event["sensor_id"])


def kafka_key_for_weather_event(event: dict[str, Any]) -> str:
    location_id = event.get("weather_location_id")
    if (
        event.get("source") != "open_meteo"
        or event.get("source_dataset") != "forecast_best_match"
        or not isinstance(location_id, str)
        or not location_id.strip()
        or location_id != location_id.strip()
    ):
        raise ValueError(
            "Weather events require the supported source, dataset, and location ID."
        )
    return f"open_meteo:{location_id}"


def produce_events(
    events: Iterable[dict[str, Any]],
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    topic: str = KAFKA_TOPIC,
    *,
    key_for_event: Callable[[dict[str, Any]], str] = kafka_key_for_event,
    delivery_timeout_seconds: float = 30.0,
) -> int:
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    produced_count = 0
    delivery_errors = []

    def on_delivery(error, message) -> None:
        if error is not None:
            delivery_errors.append(str(error))

    for event in events:
        producer.produce(
            topic=topic,
            key=key_for_event(event),
            value=json.dumps(event, ensure_ascii=False, separators=(",", ":")),
            on_delivery=on_delivery,
        )
        producer.poll(0)
        produced_count += 1

    undelivered_count = producer.flush(delivery_timeout_seconds)
    if undelivered_count:
        raise RuntimeError(
            f"Kafka producer could not deliver {undelivered_count} events."
        )
    if delivery_errors:
        raise RuntimeError(f"Kafka delivery failed: {delivery_errors[0]}")

    return produced_count


def produce_weather_events(
    events: Iterable[dict[str, Any]],
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
) -> int:
    return produce_events(
        events,
        bootstrap_servers=bootstrap_servers,
        topic=WEATHER_KAFKA_TOPIC,
        key_for_event=kafka_key_for_weather_event,
    )
