import argparse
import json
import os
from pathlib import Path
from typing import Any

import requests


OPENAQ_API_BASE_URL = "https://api.openaq.org/v3"
DEFAULT_LOCATION_ID = 2178
LATEST_OUTPUT_PATH = Path("data/input/openaq_location_latest_raw.json")
SENSOR_OUTPUT_DIR = Path("data/input/openaq_sensors")


def get_required_env_var(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Environment variable {name} must be set.")

    return value


def fetch_json(
    url: str,
    api_key: str,
    timeout_seconds: int = 30,
) -> dict[str, Any]:
    if not api_key:
        raise ValueError("OpenAQ API key must not be empty.")

    response = requests.get(
        url,
        headers={"X-API-Key": api_key},
        timeout=timeout_seconds,
    )
    response.raise_for_status()

    return response.json()


def latest_measurements_url(location_id: int) -> str:
    return f"{OPENAQ_API_BASE_URL}/locations/{location_id}/latest?limit=100"


def sensor_metadata_url(sensor_id: int) -> str:
    return f"{OPENAQ_API_BASE_URL}/sensors/{sensor_id}"


def extract_sensor_ids(latest_payload: dict[str, Any]) -> list[int]:
    return sorted(
        {
            result["sensorsId"]
            for result in latest_payload.get("results", [])
            if result.get("sensorsId") is not None
        }
    )


def write_json(payload: dict[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )


def ingest_location_latest(location_id: int, api_key: str) -> None:
    latest_payload = fetch_json(
        latest_measurements_url(location_id),
        api_key=api_key,
    )
    write_json(latest_payload, LATEST_OUTPUT_PATH)

    for sensor_id in extract_sensor_ids(latest_payload):
        sensor_payload = fetch_json(
            sensor_metadata_url(sensor_id),
            api_key=api_key,
        )
        write_json(sensor_payload, SENSOR_OUTPUT_DIR / f"sensor_{sensor_id}.json")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download OpenAQ latest measurements and sensor metadata as raw JSON."
    )
    parser.add_argument(
        "--location-id",
        type=int,
        default=DEFAULT_LOCATION_ID,
        help=f"OpenAQ location ID to ingest. Defaults to {DEFAULT_LOCATION_ID}.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    api_key = get_required_env_var("OPENAQ_API_KEY")
    ingest_location_latest(args.location_id, api_key)


if __name__ == "__main__":
    main()
