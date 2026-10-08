import argparse
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from environmental_streaming.ingestion.weather_client import (
    OPEN_METEO_HOURLY_UNITS,
    WEATHER_CAPTURE_DIRECTORY,
    HourlyWeatherResponse,
    validate_hourly_response,
)
from environmental_streaming.ingestion.weather_locations import WeatherLocation

CANONICAL_HOURLY_UNITS = {
    **OPEN_METEO_HOURLY_UNITS,
    "temperature_2m": "degC",
    "wind_direction_10m": "degree",
}


def build_canonical_weather_events(
    location: WeatherLocation, response: HourlyWeatherResponse
) -> list[dict[str, Any]]:
    validate_hourly_response(response.payload)
    fetched = response.fetched_at_utc
    if fetched.utcoffset() != timedelta(0):
        raise ValueError("fetched_at_utc must have an explicit UTC offset.")

    hourly = response.payload["hourly"]
    events = []
    seen_hours: set[datetime] = set()
    for index, label in enumerate(hourly["time"]):
        # Offset-free source labels are UTC only after envelope validation.
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:00", label) is None:
            raise ValueError(f"Invalid Open-Meteo hourly time label: {label}")
        measured = datetime.fromisoformat(label).replace(tzinfo=timezone.utc)
        if measured in seen_hours:
            raise ValueError(f"Duplicate Open-Meteo hourly time label: {label}")
        seen_hours.add(measured)
        if measured > fetched:
            continue
        for parameter, unit in CANONICAL_HOURLY_UNITS.items():
            events.append(
                {
                    "source": "open_meteo",
                    "source_dataset": "forecast_best_match",
                    "weather_location_id": location.weather_location_id,
                    "city": location.city,
                    "latitude": location.latitude,
                    "longitude": location.longitude,
                    "grid_latitude": response.payload.get("latitude"),
                    "grid_longitude": response.payload.get("longitude"),
                    "parameter": parameter,
                    "value": hourly[parameter][index],
                    "unit": unit,
                    "measured_at_utc": measured.isoformat(),
                    "fetched_at_utc": fetched.isoformat(),
                }
            )
    return events


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export canonical weather events from a saved raw capture."
    )
    parser.add_argument("--capture-file", type=Path, required=True)
    args = parser.parse_args()
    capture = json.loads(args.capture_file.read_text(encoding="utf-8"))
    if (
        capture.get("source") != "open_meteo"
        or capture.get("source_dataset") != "forecast_best_match"
    ):
        raise ValueError("Capture must use open_meteo and forecast_best_match.")
    metadata = capture["location"]
    location = WeatherLocation(
        weather_location_id=metadata["weather_location_id"],
        city=metadata["city"],
        latitude=metadata["latitude"],
        longitude=metadata["longitude"],
        openaq_location_ids=tuple(metadata["openaq_location_ids"]),
    )
    response = HourlyWeatherResponse(
        payload=capture["response"],
        fetched_at_utc=datetime.fromisoformat(capture["fetched_at_utc"]),
    )
    events = build_canonical_weather_events(location, response)
    WEATHER_CAPTURE_DIRECTORY.mkdir(parents=True, exist_ok=True)
    output_path = WEATHER_CAPTURE_DIRECTORY / (
        f"{args.capture_file.stem}_canonical.jsonl"
    )
    with output_path.open("x", encoding="utf-8") as output:
        for event in events:
            output.write(json.dumps(event) + "\n")
    print(json.dumps({"event_count": len(events), "output_path": str(output_path)}))


if __name__ == "__main__":
    main()
