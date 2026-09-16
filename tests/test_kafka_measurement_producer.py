import json
from typing import Any

import pytest

from src import kafka_measurement_producer


def test_read_jsonl_returns_one_event_per_non_empty_line(tmp_path) -> None:
    jsonl_path = tmp_path / "events.jsonl"
    jsonl_path.write_text(
        "\n".join(
            [
                '{"sensor_id": 3916, "parameter": "no2"}',
                "",
                '{"sensor_id": 3920, "parameter": "pm25"}',
            ]
        ),
        encoding="utf-8",
    )

    result = kafka_measurement_producer.read_jsonl(jsonl_path)

    assert result == [
        {"sensor_id": 3916, "parameter": "no2"},
        {"sensor_id": 3920, "parameter": "pm25"},
    ]


def test_kafka_key_for_event_uses_sensor_id() -> None:
    result = kafka_measurement_producer.kafka_key_for_event({"sensor_id": 3918})

    assert result == "3918"


def test_produce_events_sends_sensor_id_key_and_json_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    produced_messages: list[dict[str, Any]] = []
    flush_calls = 0

    class FakeProducer:
        def __init__(self, config: dict[str, str]) -> None:
            assert config == {"bootstrap.servers": "localhost:9092"}

        def produce(self, topic: str, key: str, value: str) -> None:
            produced_messages.append({"topic": topic, "key": key, "value": value})

        def flush(self) -> None:
            nonlocal flush_calls
            flush_calls += 1

    monkeypatch.setattr(kafka_measurement_producer, "Producer", FakeProducer)

    count = kafka_measurement_producer.produce_events(
        [
            {
                "source": "openaq",
                "sensor_id": 3916,
                "parameter": "pm25",
                "value": 4.0,
                "unit": "ug/m3",
            }
        ],
        bootstrap_servers="localhost:9092",
        topic="environment.measurements.raw",
    )

    assert count == 1
    assert flush_calls == 1
    assert produced_messages == [
        {
            "topic": "environment.measurements.raw",
            "key": "3916",
            "value": json.dumps(
                {
                    "source": "openaq",
                    "sensor_id": 3916,
                    "parameter": "pm25",
                    "value": 4.0,
                    "unit": "ug/m3",
                },
                ensure_ascii=False,
                separators=(",", ":"),
            ),
        }
    ]


def test_produce_events_accepts_custom_kafka_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    produced_messages: list[dict[str, Any]] = []

    class FakeProducer:
        def __init__(self, config: dict[str, str]) -> None:
            assert config == {"bootstrap.servers": "broker:9092"}

        def produce(self, topic: str, key: str, value: str) -> None:
            produced_messages.append({"topic": topic, "key": key, "value": value})

        def flush(self) -> None:
            return None

    monkeypatch.setattr(kafka_measurement_producer, "Producer", FakeProducer)

    count = kafka_measurement_producer.produce_events(
        [{"sensor_id": 3918, "value": 0.0004}],
        bootstrap_servers="broker:9092",
        topic="custom.measurements",
    )

    assert count == 1
    assert produced_messages[0]["topic"] == "custom.measurements"
    assert produced_messages[0]["key"] == "3918"


def test_parse_args_returns_producer_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "kafka_measurement_producer.py",
            "--input-path",
            "data/example.jsonl",
            "--bootstrap-servers",
            "broker:9092",
            "--topic",
            "custom.measurements",
        ],
    )

    result = kafka_measurement_producer.parse_args()

    assert result == kafka_measurement_producer.ProducerConfig(
        input_path=kafka_measurement_producer.Path("data/example.jsonl"),
        bootstrap_servers="broker:9092",
        topic="custom.measurements",
    )
