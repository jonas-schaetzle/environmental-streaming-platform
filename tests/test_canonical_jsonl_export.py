import json

import pytest
from pyspark.sql import SparkSession

from src import canonical_jsonl_export


def test_write_jsonl_creates_parent_directory_and_writes_lines(tmp_path) -> None:
    output_path = tmp_path / "nested" / "events.jsonl"

    canonical_jsonl_export.write_jsonl(
        ['{"sensor_id":3916}', '{"sensor_id":3920}'],
        output_path,
    )

    assert output_path.read_text(encoding="utf-8") == (
        '{"sensor_id":3916}\n{"sensor_id":3920}\n'
    )


def test_export_canonical_measurements_writes_jsonl(
    spark: SparkSession,
    tmp_path,
) -> None:
    input_path = tmp_path / "canonical_measurements"
    output_path = tmp_path / "events" / "openaq.jsonl"
    measurements = spark.createDataFrame(
        [
            ("openaq", 2178, 3916, "no2", "NO2", 0.007, "ppm"),
            ("openaq", 2178, 3920, "pm25", "PM2.5", 4.0, "ug/m3"),
        ],
        [
            "source",
            "location_id",
            "sensor_id",
            "parameter",
            "parameter_display_name",
            "value",
            "unit",
        ],
    )
    measurements.write.parquet(str(input_path))

    result = canonical_jsonl_export.export_canonical_measurements(
        spark,
        input_path=input_path,
        output_path=output_path,
    )

    exported_events = [
        json.loads(line)
        for line in output_path.read_text(encoding="utf-8").splitlines()
    ]

    assert result == 2
    assert sorted(exported_events, key=lambda event: event["sensor_id"]) == [
        {
            "source": "openaq",
            "location_id": 2178,
            "sensor_id": 3916,
            "parameter": "no2",
            "parameter_display_name": "NO2",
            "value": 0.007,
            "unit": "ppm",
        },
        {
            "source": "openaq",
            "location_id": 2178,
            "sensor_id": 3920,
            "parameter": "pm25",
            "parameter_display_name": "PM2.5",
            "value": 4.0,
            "unit": "ug/m3",
        },
    ]


def test_parse_args_returns_export_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "canonical_jsonl_export.py",
            "--input-path",
            "data/output/custom",
            "--output-path",
            "data/stream/input/custom.jsonl",
        ],
    )

    result = canonical_jsonl_export.parse_args()

    assert result == canonical_jsonl_export.ExportConfig(
        input_path=canonical_jsonl_export.Path("data/output/custom"),
        output_path=canonical_jsonl_export.Path("data/stream/input/custom.jsonl"),
    )
