import argparse
import json
import math
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from environmental_streaming.ingestion.weather_locations import (
    DEFAULT_WEATHER_LOCATIONS_PATH,
    WeatherLocation,
    load_weather_locations,
)

OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
DEFAULT_REQUEST_MAX_ATTEMPTS = 4
DEFAULT_REQUEST_BACKOFF_SECONDS = 1.0
DEFAULT_PAST_HOURS = 3
WEATHER_CAPTURE_DIRECTORY = Path("data/weather")
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
OPEN_METEO_HOURLY_UNITS = {
    "temperature_2m": "\u00b0C",
    "relative_humidity_2m": "%",
    "precipitation": "mm",
    "wind_speed_10m": "m/s",
    "wind_direction_10m": "\u00b0",
    "pressure_msl": "hPa",
}


@dataclass(frozen=True)
class HourlyWeatherResponse:
    payload: dict[str, Any]
    fetched_at_utc: datetime


def fetch_hourly_weather(
    location: WeatherLocation,
    past_hours: int = DEFAULT_PAST_HOURS,
    timeout_seconds: float = 30,
    max_attempts: int = DEFAULT_REQUEST_MAX_ATTEMPTS,
    backoff_seconds: float = DEFAULT_REQUEST_BACKOFF_SECONDS,
) -> HourlyWeatherResponse:
    if type(past_hours) is not int or past_hours < 1:
        raise ValueError("past_hours must be a positive integer.")
    if type(max_attempts) is not int or max_attempts < 1:
        raise ValueError("max_attempts must be a positive integer.")
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be finite and positive.")
    if not math.isfinite(backoff_seconds) or backoff_seconds < 0:
        raise ValueError("backoff_seconds must be finite and non-negative.")

    params = {
        "latitude": location.latitude,
        "longitude": location.longitude,
        "hourly": ",".join(OPEN_METEO_HOURLY_UNITS),
        "timezone": "UTC",
        "timeformat": "iso8601",
        "temperature_unit": "celsius",
        "wind_speed_unit": "ms",
        "precipitation_unit": "mm",
        "past_hours": past_hours,
        "forecast_hours": 1,
    }
    for attempt in range(max_attempts):
        try:
            response = requests.get(
                OPEN_METEO_FORECAST_URL, params=params, timeout=timeout_seconds
            )
            response.raise_for_status()
        except (
            requests.ConnectionError,
            requests.Timeout,
            requests.HTTPError,
        ) as error:
            status_code = getattr(error.response, "status_code", None)
            retryable = status_code is None or status_code in RETRYABLE_STATUS_CODES
            if not retryable or attempt == max_attempts - 1:
                raise
            time.sleep(backoff_seconds * (2**attempt))
        else:
            fetched_at_utc = datetime.now(timezone.utc)
            payload = response.json()
            _validate_hourly_response(payload)
            return HourlyWeatherResponse(payload, fetched_at_utc)

    raise RuntimeError("Open-Meteo request retry loop ended unexpectedly.")


def _validate_hourly_response(payload: Any) -> None:
    if not isinstance(payload, dict):
        raise ValueError("Open-Meteo must return a JSON object.")
    if (
        type(payload.get("utc_offset_seconds")) is not int
        or payload["utc_offset_seconds"] != 0
    ):
        raise ValueError("Open-Meteo response must have a zero UTC offset.")
    hourly = payload.get("hourly")
    units = payload.get("hourly_units")
    if not isinstance(hourly, dict) or not isinstance(units, dict):
        raise ValueError(
            "Open-Meteo response must contain hourly and hourly_units objects."
        )
    times = hourly.get("time")
    if (
        not isinstance(times, list)
        or not times
        or any(not isinstance(value, str) or not value.strip() for value in times)
    ):
        raise ValueError(
            "Open-Meteo response must contain non-empty hourly time labels."
        )
    if units.get("time") != "iso8601":
        raise ValueError("Open-Meteo response must use ISO 8601 hourly time labels.")
    for parameter, expected_unit in OPEN_METEO_HOURLY_UNITS.items():
        values = hourly.get(parameter)
        if not isinstance(values, list) or len(values) != len(times):
            raise ValueError(
                f"Open-Meteo {parameter} values must align with hourly times."
            )
        if units.get(parameter) != expected_unit:
            raise ValueError(f"Open-Meteo {parameter} must use {expected_unit}.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Capture one Open-Meteo hourly response."
    )
    parser.add_argument("--weather-location-id", required=True)
    parser.add_argument(
        "--locations-file", type=Path, default=DEFAULT_WEATHER_LOCATIONS_PATH
    )
    parser.add_argument("--past-hours", type=int, default=DEFAULT_PAST_HOURS)
    args = parser.parse_args()
    locations = load_weather_locations(args.locations_file)
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

    result = fetch_hourly_weather(location, past_hours=args.past_hours)
    snapshot = {
        "source": "open_meteo",
        "source_dataset": "forecast_best_match",
        "location": asdict(location),
        "fetched_at_utc": result.fetched_at_utc.isoformat(),
        "response": result.payload,
    }
    WEATHER_CAPTURE_DIRECTORY.mkdir(parents=True, exist_ok=True)
    output_path = WEATHER_CAPTURE_DIRECTORY / (
        f"open_meteo_{result.fetched_at_utc:%Y%m%dT%H%M%S%fZ}.json"
    )
    with output_path.open("x", encoding="utf-8") as output:
        json.dump(snapshot, output, indent=2)
        output.write("\n")
    print(
        json.dumps(
            {
                "weather_location_id": location.weather_location_id,
                "fetched_at_utc": result.fetched_at_utc.isoformat(),
                "hour_count": len(result.payload["hourly"]["time"]),
                "output_path": str(output_path),
            }
        )
    )


if __name__ == "__main__":
    main()
