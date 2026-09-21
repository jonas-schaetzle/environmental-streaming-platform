import json
from typing import Any

import pytest

from src import kafka_publisher


def test_produce_events_uses_sensor_id_as_key_and_serializes_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    produced_messages: list[dict[str, Any]] = []

    class FakeProducer:
        def __init__(self, config: dict[str, str]) -> None:
            assert config == {"bootstrap.servers": "localhost:9092"}

        def produce(self, topic: str, key: str, value: str) -> None:
            produced_messages.append({"topic": topic, "key": key, "value": value})

        def flush(self) -> int:
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

        def produce(self, topic: str, key: str, value: str) -> None:
            pass

        def flush(self) -> int:
            return 1

    monkeypatch.setattr(kafka_publisher, "Producer", FakeProducer)

    with pytest.raises(RuntimeError, match="could not deliver 1 events"):
        kafka_publisher.produce_events([{"sensor_id": 3916}])
