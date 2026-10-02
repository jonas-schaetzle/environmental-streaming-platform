import json
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import unix_micros
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from environmental_streaming.processing.weather_model import (
    WEATHER_IDENTITY_COLUMNS,
    canonical_weather_schema,
    filter_invalid_weather_observations,
    filter_valid_weather_observations,
    parse_weather_observations,
)


def _weather_payload(**overrides: object) -> dict[str, object]:
    return {
        "source": "open_meteo",
        "source_dataset": "forecast_best_match",
        "weather_location_id": "munich",
        "city": "Munich",
        "latitude": 48.137,
        "longitude": 11.575,
        "grid_latitude": 48.14,
        "grid_longitude": 11.58,
        "parameter": "temperature_2m",
        "value": -5.0,
        "unit": "degC",
        "measured_at_utc": "2026-10-02T10:00:00Z",
        "fetched_at_utc": "2026-10-02T10:15:00.123456Z",
        **overrides,
    }


def _payloads(spark: SparkSession, cases: dict[str, str | None]) -> DataFrame:
    schema = StructType(
        [
            StructField("case", StringType()),
            StructField("kafka_topic", StringType()),
            StructField("kafka_partition", IntegerType()),
            StructField("kafka_offset", LongType()),
            StructField("kafka_timestamp", TimestampType()),
            StructField("kafka_key", StringType()),
            StructField("raw_value", StringType()),
        ]
    )
    return spark.createDataFrame(
        [
            (
                name,
                "environment.weather.canonical",
                1,
                offset,
                datetime(2026, 10, 2, 10, 16, tzinfo=timezone.utc),
                "open_meteo:munich",
                payload,
            )
            for offset, (name, payload) in enumerate(cases.items())
        ],
        schema,
    )


def test_weather_schema_defines_business_identity_and_required_types() -> None:
    schema = canonical_weather_schema()
    assert schema.fieldNames() == [
        "source",
        "source_dataset",
        "weather_location_id",
        "city",
        "latitude",
        "longitude",
        "grid_latitude",
        "grid_longitude",
        "parameter",
        "value",
        "unit",
        "measured_at_utc",
        "fetched_at_utc",
    ]
    assert all(not field.nullable for field in schema)
    assert isinstance(schema["weather_location_id"].dataType, StringType)
    assert isinstance(schema["value"].dataType, DoubleType)
    assert isinstance(schema["measured_at_utc"].dataType, TimestampType)
    assert isinstance(schema["fetched_at_utc"].dataType, TimestampType)
    assert WEATHER_IDENTITY_COLUMNS == (
        "source",
        "source_dataset",
        "weather_location_id",
        "parameter",
        "measured_at_utc",
    )


def test_valid_weather_accepts_parameter_boundaries_and_preserves_lineage(
    spark: SparkSession,
) -> None:
    values = [
        ("temperature_2m", -5.0, "degC"),
        ("relative_humidity_2m", 0.0, "%"),
        ("relative_humidity_2m", 100.0, "%"),
        ("precipitation", 0.0, "mm"),
        ("wind_speed_10m", 0.0, "m/s"),
        ("wind_direction_10m", 0.0, "degree"),
        ("wind_direction_10m", 360.0, "degree"),
        ("pressure_msl", 1013.25, "hPa"),
    ]
    cases = {
        str(index): json.dumps(
            _weather_payload(parameter=parameter, value=value, unit=unit)
        )
        for index, (parameter, value, unit) in enumerate(values)
    }
    parsed = parse_weather_observations(_payloads(spark, cases))
    rows = (
        filter_valid_weather_observations(parsed)
        .withColumn("kafka_time_micros", unix_micros("kafka_timestamp"))
        .orderBy("kafka_offset")
        .collect()
    )
    assert len(rows) == len(cases)
    assert [row.value for row in rows] == [value for _, value, _ in values]
    assert filter_invalid_weather_observations(parsed).count() == 0
    assert "validation_error" not in rows[0].asDict()
    assert isinstance(parsed.schema["measured_at_utc"].dataType, TimestampType)
    for index, row in enumerate(rows):
        assert row.kafka_topic == "environment.weather.canonical"
        assert row.kafka_partition == 1
        assert row.kafka_offset == index
        assert row.kafka_key == "open_meteo:munich"
        assert row.kafka_time_micros == (
            int(datetime(2026, 10, 2, 10, 16, tzinfo=timezone.utc).timestamp()) * 10**6
        )
        assert row.raw_value == cases[str(index)]
        assert row.latitude != row.grid_latitude


def test_missing_and_malformed_weather_is_quarantined_without_losing_input(
    spark: SparkSession,
) -> None:
    cases: dict[str, str | None] = {}
    expected = {}
    for field in canonical_weather_schema().fieldNames():
        payload = _weather_payload()
        del payload[field]
        cases[field] = json.dumps(payload)
        expected[field] = f"missing_{field}"
    for field in ("source", "source_dataset", "weather_location_id", "city", "unit"):
        name = f"blank_{field}"
        cases[name] = json.dumps(_weather_payload(**{field: "  "}))
        expected[name] = f"missing_{field}"
    cases.update(
        {
            "null_value": json.dumps(_weather_payload(value=None)),
            "numeric_type_error": json.dumps(_weather_payload(value="not a number")),
            "malformed": '{"source":',
            "truncated": json.dumps(_weather_payload())[:-1],
            "array": json.dumps([_weather_payload()]),
            "scalar": '"weather"',
            "json_null": "null",
            "empty": "",
            "missing_payload": None,
        }
    )
    expected["null_value"] = "missing_value"
    for name in (
        "numeric_type_error",
        "malformed",
        "truncated",
        "array",
        "scalar",
        "json_null",
        "empty",
        "missing_payload",
    ):
        expected[name] = "invalid_payload"
    parsed = parse_weather_observations(_payloads(spark, cases))
    rows = (
        filter_invalid_weather_observations(parsed)
        .withColumn("kafka_time_micros", unix_micros("kafka_timestamp"))
        .collect()
    )
    assert len(rows) == len(cases)
    assert filter_valid_weather_observations(parsed).count() == 0
    for row in rows:
        assert expected[row.case] in row.validation_error.split(",")
        assert row.raw_value == cases[row.case]
        assert row.kafka_topic == "environment.weather.canonical"
        assert row.kafka_partition == 1
        assert row.kafka_offset == list(cases).index(row.case)
        assert row.kafka_time_micros == (
            int(datetime(2026, 10, 2, 10, 16, tzinfo=timezone.utc).timestamp()) * 10**6
        )
        assert row.kafka_key == "open_meteo:munich"
    missing_value = next(row for row in rows if row.case == "null_value")
    assert missing_value.value is None


def test_weather_rejects_non_finite_values_wrong_units_and_invalid_ranges(
    spark: SparkSession,
) -> None:
    scenarios = {
        "source": ({"source": "openaq"}, "unsupported_source"),
        "dataset": ({"source_dataset": "era5"}, "unsupported_source_dataset"),
        "parameter": ({"parameter": "pm25"}, "unsupported_parameter"),
        "unit": ({"unit": "fahrenheit"}, "invalid_unit"),
        "nan": ({"value": float("nan")}, "non_finite_value"),
        "infinity": ({"value": float("inf")}, "non_finite_value"),
        "negative_infinity": ({"value": -float("inf")}, "non_finite_value"),
    }
    for parameter, value, unit in (
        ("relative_humidity_2m", -0.1, "%"),
        ("relative_humidity_2m", 100.1, "%"),
        ("precipitation", -0.1, "mm"),
        ("wind_speed_10m", -0.1, "m/s"),
        ("wind_direction_10m", -0.1, "degree"),
        ("wind_direction_10m", 360.1, "degree"),
        ("pressure_msl", 0.0, "hPa"),
        ("pressure_msl", -1.0, "hPa"),
    ):
        scenarios[f"{parameter}_{value}"] = (
            {"parameter": parameter, "value": value, "unit": unit},
            "value_out_of_range",
        )
    for field, limit in (
        ("latitude", 90),
        ("longitude", 180),
        ("grid_latitude", 90),
        ("grid_longitude", 180),
    ):
        for value in (-limit - 1.0, limit + 1.0, float("nan"), float("inf")):
            scenarios[f"{field}_{value}"] = ({field: value}, f"invalid_{field}")
    cases = {
        name: json.dumps(_weather_payload(**overrides))
        for name, (overrides, _) in scenarios.items()
    }
    parsed = parse_weather_observations(_payloads(spark, cases))
    rows = filter_invalid_weather_observations(parsed).collect()
    assert len(rows) == len(cases)
    assert filter_valid_weather_observations(parsed).count() == 0
    for row in rows:
        assert row.validation_error == scenarios[row.case][1]


def test_weather_utc_parsing_is_explicit_hourly_and_replay_safe(
    spark: SparkSession,
) -> None:
    session = spark.newSession()
    session.conf.set("spark.sql.session.timeZone", "Europe/Berlin")
    session.conf.set("spark.sql.ansi.enabled", "true")
    cases = {
        "z": json.dumps(_weather_payload()),
        "zero_offset": json.dumps(
            _weather_payload(
                measured_at_utc="2026-10-02T10:00:00.000000+00:00",
                fetched_at_utc="2026-10-02T10:15:00.1+00:00",
            )
        ),
        "old_replay": json.dumps(
            _weather_payload(
                measured_at_utc="2020-01-01T10:00:00Z",
                fetched_at_utc="2020-01-01T10:15:00Z",
            )
        ),
    }
    scenarios = {
        "naive": (
            {"measured_at_utc": "2026-10-02T10:00:00"},
            "invalid_measured_at_utc",
        ),
        "local_offset": (
            {"measured_at_utc": "2026-10-02T12:00:00+02:00"},
            "invalid_measured_at_utc",
        ),
        "invalid_date": (
            {"measured_at_utc": "2026-02-30T10:00:00Z"},
            "invalid_measured_at_utc",
        ),
        "non_hourly": (
            {"measured_at_utc": "2026-10-02T10:01:00Z"},
            "non_hourly_measured_at_utc",
        ),
        "fractional_hour": (
            {"measured_at_utc": "2026-10-02T10:00:00.000001Z"},
            "non_hourly_measured_at_utc",
        ),
        "future": (
            {"measured_at_utc": "2026-10-02T11:00:00Z"},
            "future_measured_at_utc",
        ),
        "naive_fetch": (
            {"fetched_at_utc": "2026-10-02T10:15:00"},
            "invalid_fetched_at_utc",
        ),
        "local_fetch": (
            {"fetched_at_utc": "2026-10-02T12:15:00+02:00"},
            "invalid_fetched_at_utc",
        ),
        "invalid_fetch": (
            {"fetched_at_utc": "not a timestamp"},
            "invalid_fetched_at_utc",
        ),
        "blank_time": ({"measured_at_utc": " "}, "missing_measured_at_utc"),
    }
    cases.update(
        {
            name: json.dumps(_weather_payload(**overrides))
            for name, (overrides, _) in scenarios.items()
        }
    )
    parsed = parse_weather_observations(_payloads(session, cases))
    valid = (
        filter_valid_weather_observations(parsed)
        .select(
            "case",
            unix_micros("measured_at_utc").alias("measured"),
            unix_micros("fetched_at_utc").alias("fetched"),
        )
        .collect()
    )
    rows = {row.case: row for row in valid}
    assert set(rows) == {"z", "zero_offset", "old_replay"}
    measured = int(datetime(2026, 10, 2, 10, tzinfo=timezone.utc).timestamp()) * 10**6
    fetched = (
        int(datetime(2026, 10, 2, 10, 15, tzinfo=timezone.utc).timestamp()) * 10**6
    )
    assert rows["z"].measured == rows["zero_offset"].measured == measured
    assert rows["z"].fetched == fetched + 123456
    assert rows["zero_offset"].fetched == fetched + 100000
    invalid = filter_invalid_weather_observations(parsed).collect()
    assert len(valid) + len(invalid) == len(cases)
    for row in invalid:
        assert scenarios[row.case][1] in row.validation_error.split(",")
