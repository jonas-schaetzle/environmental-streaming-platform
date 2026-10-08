import argparse
import json
from pathlib import Path
from typing import Any

from environmental_streaming.messaging.kafka_publisher import (
    KAFKA_BOOTSTRAP_SERVERS,
    WEATHER_KAFKA_TOPIC,
    kafka_key_for_weather_event,
    produce_weather_events,
)


def load_weather_events(path: Path) -> list[dict[str, Any]]:
    events = []
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), 1
    ):
        try:
            event = json.loads(line)
            if not isinstance(event, dict):
                raise ValueError("Each weather event must be a JSON object.")
            kafka_key_for_weather_event(event)
            if "measured_at_utc" not in event or "fetched_at_utc" not in event:
                raise ValueError("Weather replay requires both original timestamps.")
        except ValueError as error:
            raise ValueError(f"{path}:{line_number}: {error}") from error
        events.append(event)
    if not events:
        raise ValueError(f"{path} must contain at least one weather event.")
    return events


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay saved canonical weather events to Kafka."
    )
    parser.add_argument("--events-file", type=Path, required=True)
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS)
    args = parser.parse_args()
    events = load_weather_events(args.events_file)
    count = produce_weather_events(events, bootstrap_servers=args.bootstrap_servers)
    print(json.dumps({"topic": WEATHER_KAFKA_TOPIC, "delivered_count": count}))


if __name__ == "__main__":
    main()
