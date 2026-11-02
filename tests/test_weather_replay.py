import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from environmental_streaming.ingestion import weather_replay


def _event() -> dict:
    return {
        "source": "open_meteo",
        "source_dataset": "forecast_best_match",
        "weather_location_id": "munich",
        "parameter": "temperature_2m",
        "value": None,
        "measured_at_utc": "2026-10-06T15:00:00+00:00",
        "fetched_at_utc": "2026-10-06T15:07:42.202170+00:00",
    }


def test_replay_preserves_input_and_reports_confirmed_delivery(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = tmp_path / "events.jsonl"
    original = json.dumps(_event()) + "\n"
    path.write_text(original, encoding="utf-8")
    publish = Mock(return_value=1)
    monkeypatch.setattr(weather_replay, "produce_weather_events", publish)
    monkeypatch.setattr("sys.argv", ["weather_replay", "--events-file", str(path)])
    weather_replay.main()
    publish.assert_called_once_with([_event()], bootstrap_servers="localhost:9092")
    assert json.loads(capsys.readouterr().out) == {
        "topic": "environment.weather.canonical",
        "delivered_count": 1,
    }
    assert path.read_text(encoding="utf-8") == original
    publish.side_effect = RuntimeError("delivery failed")
    with pytest.raises(RuntimeError, match="delivery failed"):
        weather_replay.main()
    assert capsys.readouterr().out == ""


def test_replay_preflights_entire_file_before_publishing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = tmp_path / "events.jsonl"
    publish = Mock()
    monkeypatch.setattr(weather_replay, "produce_weather_events", publish)
    monkeypatch.setattr("sys.argv", ["weather_replay", "--events-file", str(path)])
    bad_events = [
        [],
        None,
        {**_event(), "source": "openaq"},
        {**_event(), "source_dataset": "era5"},
        {**_event(), "weather_location_id": ""},
    ]
    for field in ("measured_at_utc", "fetched_at_utc"):
        event = _event()
        del event[field]
        bad_events.append(event)
    for bad_line in ["", "not JSON", *(json.dumps(event) for event in bad_events)]:
        path.write_text(json.dumps(_event()) + "\n" + bad_line + "\n", encoding="utf-8")
        with pytest.raises(ValueError, match=":2:"):
            weather_replay.main()
    path.write_text("", encoding="utf-8")
    with pytest.raises(ValueError, match="at least one"):
        weather_replay.main()
    publish.assert_not_called()
