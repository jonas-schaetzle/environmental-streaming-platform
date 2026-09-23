import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from src import openaq_kafka_producer


def test_build_canonical_events_combines_measurement_and_sensor_metadata() -> None:
    latest_payload = {
        "results": [
            {
                "locationsId": 2178,
                "sensorsId": 3916,
                "value": 0.017,
                "datetime": {"utc": "2026-09-10T14:00:00Z"},
                "coordinates": {
                    "latitude": 35.1353,
                    "longitude": -106.584702,
                },
            }
        ]
    }
    sensor_payloads = [
        {
            "results": [
                {
                    "id": 3916,
                    "parameter": {
                        "name": "no2",
                        "units": "ppm",
                        "displayName": "NO2",
                    },
                }
            ]
        }
    ]

    result = openaq_kafka_producer.build_canonical_events(
        latest_payload,
        sensor_payloads,
    )

    assert result == [
        {
            "source": "openaq",
            "location_id": 2178,
            "sensor_id": 3916,
            "parameter": "no2",
            "parameter_display_name": "NO2",
            "value": 0.017,
            "unit": "ppm",
            "measured_at_utc": "2026-09-10T14:00:00Z",
            "latitude": 35.1353,
            "longitude": -106.584702,
        }
    ]


def test_select_new_events_skips_an_already_published_sensor_timestamp() -> None:
    events = [
        {
            "source": "openaq",
            "location_id": 2178,
            "sensor_id": 3916,
            "measured_at_utc": "2026-09-10T14:00:00Z",
        },
        {
            "source": "openaq",
            "location_id": 2178,
            "sensor_id": 3918,
            "measured_at_utc": "2026-09-10T14:00:00Z",
        },
    ]
    state = {"openaq:2178:3916": "2026-09-10T14:00:00Z"}

    result = openaq_kafka_producer.select_new_events(events, state)

    assert result == [events[1]]


def test_load_locations_reads_named_unique_locations(tmp_path: Path) -> None:
    locations_path = tmp_path / "locations.json"
    locations_path.write_text(
        json.dumps(
            {
                "locations": [
                    {"id": 2669, "name": "Munich Stachus"},
                    {"id": 2936, "name": "Stuttgart Bad Cannstatt"},
                ]
            }
        ),
        encoding="utf-8",
    )

    assert openaq_kafka_producer.load_locations(locations_path) == (
        openaq_kafka_producer.OpenAQLocation(2669, "Munich Stachus"),
        openaq_kafka_producer.OpenAQLocation(2936, "Stuttgart Bad Cannstatt"),
    )


def test_run_cycle_publishes_new_events_and_persists_state(
    monkeypatch,
    tmp_path: Path,
) -> None:
    event = {
        "source": "openaq",
        "location_id": 2178,
        "sensor_id": 3916,
        "parameter": "no2",
        "value": 0.017,
        "unit": "ppm",
        "measured_at_utc": "2026-09-10T14:00:00Z",
    }
    produced_events: list[dict[str, Any]] = []

    monkeypatch.setattr(
        openaq_kafka_producer,
        "fetch_location_events",
        lambda location_id, api_key, sensor_payload_cache: [event],
    )

    def fake_produce_events(
        events,
        bootstrap_servers: str,
        topic: str,
    ) -> int:
        produced_events.extend(events)
        assert bootstrap_servers == "localhost:9092"
        assert topic == "environment.measurements.canonical"
        return len(events)

    monkeypatch.setattr(
        openaq_kafka_producer,
        "produce_events",
        fake_produce_events,
    )
    state_path = tmp_path / "producer-state.json"
    config = openaq_kafka_producer.OpenAQProducerConfig(
        locations=(openaq_kafka_producer.OpenAQLocation(2178, "Test location"),),
        state_path=state_path,
    )

    first_report = openaq_kafka_producer.run_cycle(config, "test-api-key")
    second_report = openaq_kafka_producer.run_cycle(config, "test-api-key")

    assert first_report.produced_event_count == 1
    assert second_report.produced_event_count == 0
    assert first_report.locations[0].fetched_event_count == 1
    assert first_report.locations[0].new_event_count == 1
    assert second_report.locations[0].new_event_count == 0
    assert produced_events == [event]
    assert openaq_kafka_producer.load_state(state_path) == {
        "openaq:2178:3916": "2026-09-10T14:00:00Z"
    }


def test_run_cycle_continues_after_one_location_request_fails(
    monkeypatch,
    tmp_path: Path,
) -> None:
    locations = (
        openaq_kafka_producer.OpenAQLocation(2669, "Munich Stachus"),
        openaq_kafka_producer.OpenAQLocation(2936, "Stuttgart Bad Cannstatt"),
    )
    event = {
        "source": "openaq",
        "location_id": 2936,
        "sensor_id": 11328,
        "measured_at_utc": "2026-09-21T13:00:00Z",
    }

    def fake_fetch_location_events(location_id, api_key, sensor_payload_cache):
        if location_id == 2669:
            raise requests.ConnectionError("temporary OpenAQ failure")
        return [event]

    monkeypatch.setattr(
        openaq_kafka_producer,
        "fetch_location_events",
        fake_fetch_location_events,
    )
    monkeypatch.setattr(
        openaq_kafka_producer,
        "produce_events",
        lambda events, bootstrap_servers, topic: len(events),
    )

    report = openaq_kafka_producer.run_cycle(
        openaq_kafka_producer.OpenAQProducerConfig(
            locations=locations,
            state_path=tmp_path / "producer-state.json",
        ),
        "test-api-key",
    )

    assert report.produced_event_count == 1
    assert report.locations[0].error == ("ConnectionError: temporary OpenAQ failure")
    assert report.locations[1].fetched_event_count == 1

    formatted = json.loads(
        openaq_kafka_producer.format_cycle_report(
            report,
            now=datetime(2026, 9, 21, 14, 0, tzinfo=timezone.utc),
        )
    )
    assert formatted["locations_succeeded"] == 1
    assert formatted["coverage_percent"] == 50.0
    assert formatted["events_fetched"] == 1
    assert formatted["events_published"] == 1
    assert formatted["locations"][1]["freshness_seconds"] == 3600
