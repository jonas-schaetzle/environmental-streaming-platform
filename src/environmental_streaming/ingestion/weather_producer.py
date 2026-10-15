import argparse
import hashlib
import json
import math
import os
import re
import tempfile
import time
from pathlib import Path
from typing import Any

import requests
from confluent_kafka import KafkaException

from environmental_streaming.ingestion.weather_client import (
    DEFAULT_PAST_HOURS,
    fetch_hourly_weather,
)
from environmental_streaming.ingestion.weather_events import (
    CANONICAL_HOURLY_UNITS,
    build_canonical_weather_events,
)
from environmental_streaming.ingestion.weather_locations import (
    DEFAULT_WEATHER_LOCATIONS_PATH,
    WeatherLocation,
    load_weather_locations,
)
from environmental_streaming.messaging.kafka_publisher import (
    KAFKA_BOOTSTRAP_SERVERS,
    WEATHER_KAFKA_TOPIC,
    produce_weather_events,
)


def weather_event_key(event: dict[str, Any]) -> str:
    return json.dumps(
        [
            event[field]
            for field in (
                "source",
                "source_dataset",
                "weather_location_id",
                "parameter",
                "measured_at_utc",
            )
        ],
        separators=(",", ":"),
    )


def weather_event_fingerprint(event: dict[str, Any]) -> str:
    content = {
        field: value for field, value in event.items() if field != "fetched_at_utc"
    }
    serialized = json.dumps(content, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def load_weather_state(path: Path, bootstrap_servers: str) -> dict[str, Any]:
    expected = {
        "version": 1,
        "topic": WEATHER_KAFKA_TOPIC,
        "bootstrap_servers": bootstrap_servers,
    }
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {**expected, "locations": {}}
    if (
        not isinstance(state, dict)
        or set(state) != {*expected, "locations"}
        or type(state["version"]) is not int
        or any(state[field] != value for field, value in expected.items())
        or not isinstance(state["locations"], dict)
    ):
        raise ValueError(
            "Weather state has an unsupported format or Kafka destination."
        )
    for location_id, fingerprints in state["locations"].items():
        if (
            not isinstance(location_id, str)
            or not location_id.strip()
            or not isinstance(fingerprints, dict)
        ):
            raise ValueError("Weather state must contain location fingerprint objects.")
        for key, fingerprint in fingerprints.items():
            try:
                identity = json.loads(key)
            except ValueError as error:
                raise ValueError("Invalid weather state business key.") from error
            if (
                not isinstance(identity, list)
                or len(identity) != 5
                or any(not isinstance(value, str) or not value for value in identity)
                or identity[:3] != ["open_meteo", "forecast_best_match", location_id]
                or identity[3] not in CANONICAL_HOURLY_UNITS
                or not isinstance(fingerprint, str)
                or re.fullmatch(r"[0-9a-f]{64}", fingerprint) is None
            ):
                raise ValueError("Invalid weather state business key or fingerprint.")
    return state


def write_weather_state(state: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as output:
            temporary_path = Path(output.name)
            json.dump(state, output, sort_keys=True, indent=2)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary_path, path)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def publish_weather_location(
    location: WeatherLocation,
    past_hours: int = DEFAULT_PAST_HOURS,
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    *,
    state_path: Path | None = None,
) -> dict[str, Any]:
    state = (
        load_weather_state(state_path, bootstrap_servers)
        if state_path is not None
        else None
    )
    response = fetch_hourly_weather(location, past_hours=past_hours)
    events = build_canonical_weather_events(location, response)
    if not events:
        raise ValueError("Open-Meteo response contains no publishable UTC hours.")
    fingerprints = {
        weather_event_key(event): weather_event_fingerprint(event) for event in events
    }
    known = (
        state["locations"].get(location.weather_location_id, {})
        if state is not None
        else {}
    )
    pending = [
        event
        for event in events
        if known.get(weather_event_key(event)) != fingerprints[weather_event_key(event)]
    ]
    delivered_count = (
        produce_weather_events(pending, bootstrap_servers=bootstrap_servers)
        if pending
        else 0
    )
    if delivered_count != len(pending):
        raise RuntimeError(
            "Kafka delivery count does not match the pending weather batch."
        )
    if state is not None:
        # Replace only this successful location's active range after delivery.
        state["locations"][location.weather_location_id] = fingerprints
        write_weather_state(state, state_path)
    report = {
        "weather_location_id": location.weather_location_id,
        "fetched_at_utc": response.fetched_at_utc.isoformat(),
        "hour_count": len({event["measured_at_utc"] for event in events}),
        "topic": WEATHER_KAFKA_TOPIC,
        "delivered_count": delivered_count,
    }
    if state is not None:
        report["skipped_count"] = len(events) - len(pending)
    return report


def publish_weather_locations(
    locations: tuple[WeatherLocation, ...],
    past_hours: int = DEFAULT_PAST_HOURS,
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    *,
    state_path: Path | None = None,
) -> dict[str, Any]:
    if not locations:
        raise ValueError("At least one weather location is required.")
    reports = []
    errors = []
    options = {"past_hours": past_hours, "bootstrap_servers": bootstrap_servers}
    if state_path is not None:
        load_weather_state(state_path, bootstrap_servers)
        options["state_path"] = state_path
    for location in locations:
        try:
            report = publish_weather_location(location, **options)
        except (
            requests.RequestException,
            ValueError,
            RuntimeError,
            KafkaException,
            BufferError,
        ) as error:
            errors.append(
                {
                    "weather_location_id": location.weather_location_id,
                    "error_type": type(error).__name__,
                    "error": str(error),
                }
            )
        else:
            reports.append(report)
    report = {
        "topic": WEATHER_KAFKA_TOPIC,
        "locations_total": len(locations),
        "locations_succeeded": len(reports),
        "locations_failed": len(errors),
        "delivered_count": sum(report["delivered_count"] for report in reports),
        "locations": reports,
        "errors": errors,
    }
    if state_path is not None:
        report["skipped_count"] = sum(item["skipped_count"] for item in reports)
    return report


def run_weather_polling(
    locations: tuple[WeatherLocation, ...],
    state_path: Path,
    poll_interval_seconds: float,
    max_cycles: int | None = None,
    past_hours: int = DEFAULT_PAST_HOURS,
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
) -> bool:
    if not math.isfinite(poll_interval_seconds) or poll_interval_seconds <= 0:
        raise ValueError("Polling interval must be finite and positive.")
    if max_cycles is not None and (type(max_cycles) is not int or max_cycles < 1):
        raise ValueError("max_cycles must be a positive integer.")
    if type(past_hours) is not int or past_hours < 1:
        raise ValueError("past_hours must be a positive integer.")
    if state_path is None:
        raise ValueError("Weather polling requires a state path.")
    cycle = 0
    had_failures = False
    while True:
        report = publish_weather_locations(
            locations,
            past_hours=past_hours,
            bootstrap_servers=bootstrap_servers,
            state_path=state_path,
        )
        cycle += 1
        print(json.dumps({**report, "cycle": cycle}), flush=True)
        had_failures = had_failures or report["locations_failed"] > 0
        if max_cycles is not None and cycle >= max_cycles:
            return had_failures
        time.sleep(poll_interval_seconds)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch and publish hourly weather for configured locations."
    )
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--weather-location-id")
    selection.add_argument("--all-locations", action="store_true")
    parser.add_argument(
        "--locations-file", type=Path, default=DEFAULT_WEATHER_LOCATIONS_PATH
    )
    parser.add_argument("--past-hours", type=int, default=DEFAULT_PAST_HOURS)
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--state-path", type=Path)
    parser.add_argument("--poll-interval-seconds", type=float, default=0)
    parser.add_argument("--max-cycles", type=int)
    args = parser.parse_args()
    if not math.isfinite(args.poll_interval_seconds) or args.poll_interval_seconds < 0:
        parser.error("--poll-interval-seconds must be finite and zero or greater")
    if args.max_cycles is not None and (
        args.max_cycles < 1 or args.poll_interval_seconds == 0
    ):
        parser.error("--max-cycles requires a positive count and polling interval")
    if args.poll_interval_seconds > 0 and args.state_path is None:
        parser.error("Weather polling requires --state-path")
    if args.past_hours < 1:
        parser.error("--past-hours must be positive")
    locations = load_weather_locations(args.locations_file)
    if not args.all_locations:
        location = next(
            (
                item
                for item in locations
                if item.weather_location_id == args.weather_location_id
            ),
            None,
        )
        if location is None:
            parser.error(f"Unknown weather location id: {args.weather_location_id}")
        locations = (location,)
    options = {
        "past_hours": args.past_hours,
        "bootstrap_servers": args.bootstrap_servers,
    }
    if args.state_path is not None:
        options["state_path"] = args.state_path
    if args.poll_interval_seconds > 0:
        try:
            had_failures = run_weather_polling(
                locations,
                poll_interval_seconds=args.poll_interval_seconds,
                max_cycles=args.max_cycles,
                **options,
            )
        except KeyboardInterrupt:
            raise SystemExit(130) from None
        if had_failures:
            raise SystemExit(1)
        return
    if args.all_locations:
        report = publish_weather_locations(locations, **options)
        print(json.dumps(report))
        if report["locations_failed"]:
            raise SystemExit(1)
        return
    report = publish_weather_location(locations[0], **options)
    print(json.dumps(report))


if __name__ == "__main__":
    main()
