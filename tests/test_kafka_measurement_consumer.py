import json
from typing import Any

from src import kafka_measurement_consumer


class FakeMessage:
    def __init__(
        self,
        key: bytes | None,
        value: bytes,
        topic: str = "environment.measurements.raw",
        partition: int = 2,
        offset: int = 4,
    ) -> None:
        self._key = key
        self._value = value
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def key(self) -> bytes | None:
        return self._key

    def value(self) -> bytes:
        return self._value

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def error(self) -> None:
        return None


def test_decode_message_returns_kafka_metadata_and_json_value() -> None:
    message = FakeMessage(
        key=b"3916",
        value=json.dumps({"sensor_id": 3916, "value": 0.007}).encode("utf-8"),
    )

    result = kafka_measurement_consumer.decode_message(message)

    assert result == {
        "topic": "environment.measurements.raw",
        "partition": 2,
        "offset": 4,
        "key": "3916",
        "value": {"sensor_id": 3916, "value": 0.007},
    }


def test_consume_events_polls_until_timeout(monkeypatch) -> None:
    closed = False

    class FakeConsumer:
        def __init__(self, config: dict[str, str | bool]) -> None:
            assert config == {
                "bootstrap.servers": "localhost:9092",
                "group.id": "test-group",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
            self.messages = [
                FakeMessage(
                    key=b"3920",
                    value=json.dumps({"sensor_id": 3920}).encode("utf-8"),
                    offset=0,
                ),
                None,
            ]

        def subscribe(self, topics: list[str]) -> None:
            assert topics == ["environment.measurements.raw"]

        def poll(self, timeout: float) -> Any:
            assert timeout == 0.1
            return self.messages.pop(0)

        def close(self) -> None:
            nonlocal closed
            closed = True

    monkeypatch.setattr(kafka_measurement_consumer, "Consumer", FakeConsumer)

    result = kafka_measurement_consumer.consume_events(
        max_messages=10,
        timeout_seconds=0.1,
        group_id="test-group",
    )

    assert closed is True
    assert result == [
        {
            "topic": "environment.measurements.raw",
            "partition": 2,
            "offset": 0,
            "key": "3920",
            "value": {"sensor_id": 3920},
        }
    ]
