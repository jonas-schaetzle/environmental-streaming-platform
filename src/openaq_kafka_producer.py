import argparse
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

if __package__:
    from .kafka_publisher import (
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC,
        produce_events,
    )
    from .openaq_client import (
        DEFAULT_LOCATION_ID,
        extract_sensor_ids,
        fetch_json,
        get_required_env_var,
        latest_measurements_url,
        sensor_metadata_url,
    )
else:
    from kafka_publisher import (
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC,
        produce_events,
    )
    from openaq_client import (
        DEFAULT_LOCATION_ID,
        extract_sensor_ids,
        fetch_json,
        get_required_env_var,
        latest_measurements_url,
        sensor_metadata_url,
    )


DEFAULT_STATE_PATH = Path("data/state/openaq_kafka_producer.json")


@dataclass(frozen=True)
class OpenAQProducerConfig:
    location_ids: tuple[int, ...] = (DEFAULT_LOCATION_ID,)
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS
    topic: str = KAFKA_TOPIC
    state_path: Path = DEFAULT_STATE_PATH
    poll_interval_seconds: int = 0


def sensor_metadata_by_id(
    sensor_payloads: list[dict[str, Any]],
) -> dict[int, dict[str, Any]]:
    metadata: dict[int, dict[str, Any]] = {}

    for payload in sensor_payloads:
        for sensor in payload.get("results", []):
            sensor_id = sensor.get("id")
            if sensor_id is not None:
                metadata[sensor_id] = sensor

    return metadata


def build_canonical_events(
    latest_payload: dict[str, Any],
    sensor_payloads: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    metadata = sensor_metadata_by_id(sensor_payloads)
    events = []

    for measurement in latest_payload.get("results", []):
        sensor_id = measurement.get("sensorsId")
        sensor = metadata.get(sensor_id, {})
        parameter = sensor.get("parameter", {})
        measured_datetime = measurement.get("datetime", {})
        coordinates = measurement.get("coordinates", {})

        events.append(
            {
                "source": "openaq",
                "location_id": measurement.get("locationsId"),
                "sensor_id": sensor_id,
                "parameter": parameter.get("name"),
                "parameter_display_name": parameter.get("displayName"),
                "value": measurement.get("value"),
                "unit": parameter.get("units"),
                "measured_at_utc": measured_datetime.get("utc"),
                "latitude": coordinates.get("latitude"),
                "longitude": coordinates.get("longitude"),
            }
        )

    return events


def event_state_key(event: dict[str, Any]) -> str:
    return f"{event['source']}:{event['location_id']}:{event['sensor_id']}"


def select_new_events(
    events: list[dict[str, Any]],
    state: dict[str, str],
) -> list[dict[str, Any]]:
    return [
        event
        for event in events
        if event.get("measured_at_utc") is None
        or state.get(event_state_key(event)) != event.get("measured_at_utc")
    ]


def update_state(
    state: dict[str, str],
    published_events: list[dict[str, Any]],
) -> dict[str, str]:
    updated_state = state.copy()

    for event in published_events:
        measured_at_utc = event.get("measured_at_utc")
        if measured_at_utc is not None:
            updated_state[event_state_key(event)] = measured_at_utc

    return updated_state


def load_state(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}

    return json.loads(path.read_text(encoding="utf-8"))


def write_state(state: dict[str, str], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(state, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def fetch_location_events(
    location_id: int,
    api_key: str,
    sensor_payload_cache: dict[int, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    sensor_payload_cache = (
        sensor_payload_cache if sensor_payload_cache is not None else {}
    )
    latest_payload = fetch_json(
        latest_measurements_url(location_id),
        api_key=api_key,
    )

    sensor_payloads = []
    for sensor_id in extract_sensor_ids(latest_payload):
        if sensor_id not in sensor_payload_cache:
            sensor_payload_cache[sensor_id] = fetch_json(
                sensor_metadata_url(sensor_id),
                api_key=api_key,
            )
        sensor_payloads.append(sensor_payload_cache[sensor_id])

    return build_canonical_events(latest_payload, sensor_payloads)


def run_cycle(
    config: OpenAQProducerConfig,
    api_key: str,
    sensor_payload_cache: dict[int, dict[str, Any]] | None = None,
) -> int:
    events = [
        event
        for location_id in config.location_ids
        for event in fetch_location_events(
            location_id,
            api_key,
            sensor_payload_cache,
        )
    ]
    state = load_state(config.state_path)
    new_events = select_new_events(events, state)

    if not new_events:
        return 0

    produced_count = produce_events(
        new_events,
        bootstrap_servers=config.bootstrap_servers,
        topic=config.topic,
    )
    write_state(update_state(state, new_events), config.state_path)

    return produced_count


def run(config: OpenAQProducerConfig, api_key: str) -> None:
    sensor_payload_cache: dict[int, dict[str, Any]] = {}

    while True:
        produced_count = run_cycle(config, api_key, sensor_payload_cache)
        print(f"Produced {produced_count} new OpenAQ events to {config.topic}")

        if config.poll_interval_seconds == 0:
            return

        time.sleep(config.poll_interval_seconds)


def parse_args() -> OpenAQProducerConfig:
    parser = argparse.ArgumentParser(
        description="Fetch current OpenAQ measurements and produce them to Kafka."
    )
    parser.add_argument(
        "--location-id",
        type=int,
        action="append",
        dest="location_ids",
        help=(
            "OpenAQ location ID. Repeat the option for multiple locations. "
            f"Defaults to {DEFAULT_LOCATION_ID}."
        ),
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
    parser.add_argument(
        "--state-path",
        type=Path,
        default=DEFAULT_STATE_PATH,
        help="Local file used to avoid publishing the same sensor timestamp twice.",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=int,
        default=0,
        help="Seconds between API polls. Zero performs one cycle and exits.",
    )

    args = parser.parse_args()

    if args.poll_interval_seconds < 0:
        parser.error("--poll-interval-seconds must be zero or greater")

    return OpenAQProducerConfig(
        location_ids=tuple(args.location_ids or [DEFAULT_LOCATION_ID]),
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        state_path=args.state_path,
        poll_interval_seconds=args.poll_interval_seconds,
    )


def main() -> None:
    config = parse_args()
    api_key = get_required_env_var("OPENAQ_API_KEY")
    run(config, api_key)


if __name__ == "__main__":
    main()
