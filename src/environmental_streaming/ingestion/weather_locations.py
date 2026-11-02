import json
from dataclasses import dataclass
from pathlib import Path

DEFAULT_WEATHER_LOCATIONS_PATH = Path("config/weather_locations.json")


@dataclass(frozen=True)
class WeatherLocation:
    weather_location_id: str
    city: str
    latitude: float
    longitude: float
    openaq_location_ids: tuple[int, ...]


def load_weather_locations(path: Path) -> tuple[WeatherLocation, ...]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object.")

    configured_locations = payload.get("locations")
    if not isinstance(configured_locations, list) or not configured_locations:
        raise ValueError(f"{path} must define a non-empty locations list.")

    locations = []
    seen_weather_ids: set[str] = set()
    seen_openaq_ids: set[int] = set()
    for item in configured_locations:
        if not isinstance(item, dict):
            raise ValueError(f"Each location in {path} must be an object.")

        for field in ("weather_location_id", "city"):
            value = item.get(field)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(
                    f"Each location in {path} must have a non-empty {field}."
                )
        weather_location_id = item["weather_location_id"].strip()
        if weather_location_id in seen_weather_ids:
            raise ValueError(
                f"Duplicate weather location id {weather_location_id} in {path}."
            )

        for field, limit in (("latitude", 90), ("longitude", 180)):
            coordinate = item.get(field)
            if (
                isinstance(coordinate, bool)
                or not isinstance(coordinate, (int, float))
                or not -limit <= coordinate <= limit
            ):
                raise ValueError(
                    f"Each location in {path} must have a finite numeric {field} "
                    f"between {-limit} and {limit}."
                )

        openaq_ids = item.get("openaq_location_ids")
        if not isinstance(openaq_ids, list) or not openaq_ids:
            raise ValueError(
                f"Each location in {path} must have a non-empty "
                "openaq_location_ids list."
            )
        for location_id in openaq_ids:
            if (
                isinstance(location_id, bool)
                or not isinstance(location_id, int)
                or location_id <= 0
            ):
                raise ValueError(
                    f"Each OpenAQ location id in {path} must be a positive integer."
                )
            if location_id in seen_openaq_ids:
                raise ValueError(
                    f"OpenAQ location id {location_id} is mapped more than once "
                    f"in {path}."
                )
            seen_openaq_ids.add(location_id)

        seen_weather_ids.add(weather_location_id)
        locations.append(
            WeatherLocation(
                weather_location_id=weather_location_id,
                city=item["city"].strip(),
                latitude=float(item["latitude"]),
                longitude=float(item["longitude"]),
                openaq_location_ids=tuple(openaq_ids),
            )
        )

    return tuple(locations)
