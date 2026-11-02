import json
from pathlib import Path

import pytest

from environmental_streaming.ingestion.weather_locations import (
    DEFAULT_WEATHER_LOCATIONS_PATH,
    WeatherLocation,
    load_weather_locations,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def _location(**overrides: object) -> dict[str, object]:
    return {
        "weather_location_id": "munich",
        "city": "Munich",
        "latitude": 48.13743,
        "longitude": 11.57549,
        "openaq_location_ids": [2669],
        **overrides,
    }


def _write_config(tmp_path: Path, payload: object) -> Path:
    path = tmp_path / "weather_locations.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_curated_weather_locations_cover_current_openaq_locations() -> None:
    locations = load_weather_locations(REPOSITORY_ROOT / DEFAULT_WEATHER_LOCATIONS_PATH)
    assert locations == (
        WeatherLocation("munich", "Munich", 48.13743, 11.57549, (2669,)),
        WeatherLocation("stuttgart", "Stuttgart", 48.78232, 9.17702, (2936,)),
        WeatherLocation("hamburg", "Hamburg", 53.55073, 9.99302, (3071,)),
    )
    openaq_config = json.loads(
        (REPOSITORY_ROOT / "config/openaq_locations.json").read_text(encoding="utf-8")
    )
    assert {
        identifier
        for location in locations
        for identifier in location.openaq_location_ids
    } == {location["id"] for location in openaq_config["locations"]}


def test_loader_normalizes_labels_and_supports_multiple_stations(
    tmp_path: Path,
) -> None:
    path = _write_config(
        tmp_path,
        {
            "locations": [
                _location(
                    weather_location_id=" munich ",
                    city=" Munich ",
                    latitude=-90,
                    longitude=180,
                    openaq_location_ids=[2669, 2670],
                ),
                _location(
                    weather_location_id="hamburg",
                    city="Hamburg",
                    latitude=90,
                    longitude=-180,
                    openaq_location_ids=[3071],
                ),
            ]
        },
    )
    assert load_weather_locations(path) == (
        WeatherLocation("munich", "Munich", -90.0, 180.0, (2669, 2670)),
        WeatherLocation("hamburg", "Hamburg", 90.0, -180.0, (3071,)),
    )


def test_loader_rejects_invalid_structure_and_missing_labels(tmp_path: Path) -> None:
    for payload in (
        None,
        [],
        "weather",
        {},
        {"locations": None},
        {"locations": {}},
        {"locations": []},
        {"locations": [None]},
        {"locations": ["munich"]},
    ):
        with pytest.raises(ValueError):
            load_weather_locations(_write_config(tmp_path, payload))

    for field in ("weather_location_id", "city"):
        missing = _location()
        del missing[field]
        invalid_locations = [missing] + [
            _location(**{field: value}) for value in (None, "", " \t ", 42, True)
        ]
        for location in invalid_locations:
            with pytest.raises(ValueError, match=f"non-empty {field}"):
                load_weather_locations(
                    _write_config(tmp_path, {"locations": [location]})
                )

    path = tmp_path / "weather_locations.json"
    path.write_text('{"locations":', encoding="utf-8")
    with pytest.raises(json.JSONDecodeError):
        load_weather_locations(path)


def test_loader_rejects_invalid_coordinates(tmp_path: Path) -> None:
    for field, limit in (("latitude", 90), ("longitude", 180)):
        missing = _location()
        del missing[field]
        invalid_locations = [missing] + [
            _location(**{field: value})
            for value in (
                None,
                True,
                False,
                "48.1",
                [],
                {},
                -limit - 0.1,
                limit + 0.1,
                float("nan"),
                float("inf"),
                -float("inf"),
            )
        ]
        for location in invalid_locations:
            with pytest.raises(ValueError, match=f"finite numeric {field}"):
                load_weather_locations(
                    _write_config(tmp_path, {"locations": [location]})
                )


def test_loader_rejects_duplicate_or_invalid_station_mappings(tmp_path: Path) -> None:
    for weather_id in ("munich", " munich "):
        path = _write_config(
            tmp_path,
            {
                "locations": [
                    _location(),
                    _location(
                        weather_location_id=weather_id, openaq_location_ids=[2936]
                    ),
                ]
            },
        )
        with pytest.raises(ValueError, match="Duplicate weather location id munich"):
            load_weather_locations(path)

    for locations in (
        [_location(openaq_location_ids=[2669, 2669])],
        [
            _location(),
            _location(weather_location_id="stuttgart", openaq_location_ids=[2669]),
        ],
    ):
        with pytest.raises(ValueError, match="2669 is mapped more than once"):
            load_weather_locations(_write_config(tmp_path, {"locations": locations}))

    missing = _location()
    del missing["openaq_location_ids"]
    invalid_locations = [missing] + [
        _location(openaq_location_ids=value)
        for value in (
            None,
            [],
            2669,
            "2669",
            {},
            [0],
            [-1],
            [True],
            [False],
            [2669.0],
            ["2669"],
            [None],
        )
    ]
    for location in invalid_locations:
        with pytest.raises(ValueError, match="openaq_location_ids|positive integer"):
            load_weather_locations(_write_config(tmp_path, {"locations": [location]}))
