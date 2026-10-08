import json
from typing import Any

import pytest

from environmental_streaming.messaging import kafka_publisher


def test_produce_events_uses_sensor_id_as_key_and_serializes_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    produced_messages: list[dict[str, Any]] = []

    class FakeProducer:
        def __init__(self, config: dict[str, str]) -> None:
            assert config == {"bootstrap.servers": "localhost:9092"}

        def produce(self, topic: str, key: str, value: str, on_delivery) -> None:
            produced_messages.append({"topic": topic, "key": key, "value": value})
            on_delivery(None, None)

        def poll(self, timeout: float) -> None:
            assert timeout == 0

        def flush(self, timeout: float) -> int:
            assert timeout == 30.0
            return 0

    monkeypatch.setattr(kafka_publisher, "Producer", FakeProducer)
    event = {"sensor_id": 3916, "parameter": "pm25", "value": 4.0}

    count = kafka_publisher.produce_events([event])

    assert count == 1
    assert produced_messages == [
        {
            "topic": "environment.measurements.canonical",
            "key": "3916",
            "value": json.dumps(
                event,
                ensure_ascii=False,
                separators=(",", ":"),
            ),
        }
    ]


def test_produce_events_raises_when_messages_remain_undelivered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeProducer:
        def __init__(self, config: dict[str, str]) -> None:
            pass

        def produce(self, topic: str, key: str, value: str, on_delivery) -> None:
            pass

        def poll(self, timeout: float) -> None:
            pass

        def flush(self, timeout: float) -> int:
            return 1

    monkeypatch.setattr(kafka_publisher, "Producer", FakeProducer)

    with pytest.raises(RuntimeError, match="could not deliver 1 events"):
        kafka_publisher.produce_events([{"sensor_id": 3916}])


def test_weather_publishing_uses_dedicated_topic_and_preserves_replay_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages = []
    callbacks = []

    class FakeProducer:
        def __init__(self, config) -> None:
            assert config == {"bootstrap.servers": "test-broker:9092"}

        def produce(self, topic, key, value, on_delivery) -> None:
            messages.append((topic, key, json.loads(value)))
            callbacks.append(on_delivery)

        def poll(self, timeout) -> None:
            pass

        def flush(self, timeout) -> int:
            assert timeout == 30
            for callback in callbacks:
                callback(None, None)
            return 0

    monkeypatch.setattr(kafka_publisher, "Producer", FakeProducer)
    event = {
        "source": "open_meteo",
        "source_dataset": "forecast_best_match",
        "weather_location_id": "munich",
        "parameter": "temperature_2m",
        "value": None,
        "measured_at_utc": "2026-10-06T15:00:00+00:00",
        "fetched_at_utc": "2026-10-06T15:07:42.202170+00:00",
    }
    assert kafka_publisher.produce_weather_events([event], "test-broker:9092") == 1
    assert messages == [("environment.weather.canonical", "open_meteo:munich", event)]
    for overrides in (
        {"source": "openaq"},
        {"source_dataset": "era5"},
        {"weather_location_id": None},
        {"weather_location_id": " "},
        {"weather_location_id": " munich"},
        {"weather_location_id": 123},
    ):
        with pytest.raises(ValueError):
            kafka_publisher.kafka_key_for_weather_event({**event, **overrides})


def test_delivery_callback_failure_is_reported_even_when_queue_is_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeProducer:
        def __init__(self, config) -> None:
            self.callback = None

        def produce(self, topic, key, value, on_delivery) -> None:
            self.callback = on_delivery

        def poll(self, timeout) -> None:
            pass

        def flush(self, timeout) -> int:
            self.callback("broker rejected record", None)
            return 0

    monkeypatch.setattr(kafka_publisher, "Producer", FakeProducer)
    with pytest.raises(
        RuntimeError, match="Kafka delivery failed: broker rejected record"
    ):
        kafka_publisher.produce_events([{"sensor_id": 3916}])
