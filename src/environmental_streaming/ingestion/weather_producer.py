import argparse
import json
from pathlib import Path
from typing import Any

import requests
from confluent_kafka import KafkaException

from environmental_streaming.ingestion.weather_client import (
    DEFAULT_PAST_HOURS,
    fetch_hourly_weather,
)
from environmental_streaming.ingestion.weather_events import (
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


def publish_weather_location(
    location: WeatherLocation,
    past_hours: int = DEFAULT_PAST_HOURS,
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
) -> dict[str, Any]:
    response = fetch_hourly_weather(location, past_hours=past_hours)
    events = build_canonical_weather_events(location, response)
    if not events:
        raise ValueError("Open-Meteo response contains no publishable UTC hours.")
    delivered_count = produce_weather_events(
        events, bootstrap_servers=bootstrap_servers
    )
    return {
        "weather_location_id": location.weather_location_id,
        "fetched_at_utc": response.fetched_at_utc.isoformat(),
        "hour_count": len({event["measured_at_utc"] for event in events}),
        "topic": WEATHER_KAFKA_TOPIC,
        "delivered_count": delivered_count,
    }


def publish_weather_locations(
    locations: tuple[WeatherLocation, ...],
    past_hours: int = DEFAULT_PAST_HOURS,
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
) -> dict[str, Any]:
    if not locations:
        raise ValueError("At least one weather location is required.")
    reports = []
    errors = []
    for location in locations:
        try:
            report = publish_weather_location(
                location, past_hours=past_hours, bootstrap_servers=bootstrap_servers
            )
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
    return {
        "topic": WEATHER_KAFKA_TOPIC,
        "locations_total": len(locations),
        "locations_succeeded": len(reports),
        "locations_failed": len(errors),
        "delivered_count": sum(report["delivered_count"] for report in reports),
        "locations": reports,
        "errors": errors,
    }


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
    args = parser.parse_args()
    locations = load_weather_locations(args.locations_file)
    if args.all_locations:
        report = publish_weather_locations(
            locations,
            past_hours=args.past_hours,
            bootstrap_servers=args.bootstrap_servers,
        )
        print(json.dumps(report))
        if report["locations_failed"]:
            raise SystemExit(1)
        return
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
    report = publish_weather_location(
        location, past_hours=args.past_hours, bootstrap_servers=args.bootstrap_servers
    )
    print(json.dumps(report))


if __name__ == "__main__":
    main()
