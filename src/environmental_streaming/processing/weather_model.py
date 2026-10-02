from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    abs as spark_abs,
    col,
    concat_ws,
    from_json,
    isnan,
    lit,
    trim,
    try_to_timestamp,
    unix_micros,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

WEATHER_SOURCE = "open_meteo"
WEATHER_SOURCE_DATASET = "forecast_best_match"
WEATHER_PARAMETER_UNITS = {
    "temperature_2m": "degC",
    "relative_humidity_2m": "%",
    "precipitation": "mm",
    "wind_speed_10m": "m/s",
    "wind_direction_10m": "degree",
    "pressure_msl": "hPa",
}
WEATHER_IDENTITY_COLUMNS = (
    "source",
    "source_dataset",
    "weather_location_id",
    "parameter",
    "measured_at_utc",
)
_UTC_TIMESTAMP_PATTERN = (
    r"\A[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,6})?(?:Z|\+00:00)\z"
)
_SPARK_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]XXX"
_TIMESTAMP_COLUMNS = ("measured_at_utc", "fetched_at_utc")
_COORDINATE_LIMITS = {
    "latitude": 90,
    "longitude": 180,
    "grid_latitude": 90,
    "grid_longitude": 180,
}


def canonical_weather_schema() -> StructType:
    return StructType(
        [
            StructField("source", StringType(), nullable=False),
            StructField("source_dataset", StringType(), nullable=False),
            StructField("weather_location_id", StringType(), nullable=False),
            StructField("city", StringType(), nullable=False),
            StructField("latitude", DoubleType(), nullable=False),
            StructField("longitude", DoubleType(), nullable=False),
            StructField("grid_latitude", DoubleType(), nullable=False),
            StructField("grid_longitude", DoubleType(), nullable=False),
            StructField("parameter", StringType(), nullable=False),
            StructField("value", DoubleType(), nullable=False),
            StructField("unit", StringType(), nullable=False),
            StructField("measured_at_utc", TimestampType(), nullable=False),
            StructField("fetched_at_utc", TimestampType(), nullable=False),
        ]
    )


def parse_weather_observations(payloads: DataFrame) -> DataFrame:
    """Parse raw_value JSON, preserving input metadata and adding validation_error."""
    schema = canonical_weather_schema()
    # Keep timestamp text until its explicit UTC offset has been validated.
    payload_schema = StructType(
        [
            StructField(
                field.name,
                StringType() if field.name in _TIMESTAMP_COLUMNS else field.dataType,
                nullable=True,
            )
            for field in schema
        ]
        + [StructField("_corrupt_record", StringType(), nullable=True)]
    )
    parsed = payloads.withColumn(
        "_weather_payload",
        from_json(
            col("raw_value").cast("string"),
            payload_schema,
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
        ),
    ).select(
        "*",
        *[
            (
                try_to_timestamp(
                    col(f"_weather_payload.{field.name}"),
                    lit(_SPARK_TIMESTAMP_FORMAT),
                )
                if field.name in _TIMESTAMP_COLUMNS
                else col(f"_weather_payload.{field.name}")
            ).alias(field.name)
            for field in schema
        ],
    )
    return parsed.withColumn("validation_error", _weather_validation_errors()).drop(
        "_weather_payload"
    )


def filter_valid_weather_observations(observations: DataFrame) -> DataFrame:
    return observations.filter(col("validation_error") == "").drop("validation_error")


def filter_invalid_weather_observations(observations: DataFrame) -> DataFrame:
    return observations.filter(col("validation_error") != "")


def _finite_number(value: Column) -> Column:
    return ~isnan(value) & (spark_abs(value) < lit(float("inf")))


def _weather_validation_errors() -> Column:
    errors = [
        when(
            col("_weather_payload").isNull()
            | col("_weather_payload._corrupt_record").isNotNull(),
            lit("invalid_payload"),
        )
    ]
    for field in canonical_weather_schema():
        raw_field = col(f"_weather_payload.{field.name}")
        missing = raw_field.isNull()
        if isinstance(field.dataType, (StringType, TimestampType)):
            missing = missing | (trim(raw_field) == "")
        errors.append(when(missing, lit(f"missing_{field.name}")))

    for field, expected in (
        ("source", WEATHER_SOURCE),
        ("source_dataset", WEATHER_SOURCE_DATASET),
    ):
        errors.append(
            when(
                (trim(col(field)) != "") & (col(field) != expected),
                lit(f"unsupported_{field}"),
            )
        )

    parameter = col("parameter")
    value = col("value")
    errors.extend(
        [
            when(
                (trim(parameter) != "")
                & ~parameter.isin(list(WEATHER_PARAMETER_UNITS)),
                lit("unsupported_parameter"),
            ),
            when(value.isNotNull() & ~_finite_number(value), lit("non_finite_value")),
        ]
    )
    for name, unit in WEATHER_PARAMETER_UNITS.items():
        errors.append(
            when(
                (parameter == name) & (trim(col("unit")) != "") & (col("unit") != unit),
                lit("invalid_unit"),
            )
        )

    out_of_range = (
        ((parameter == "relative_humidity_2m") & ~value.between(0, 100))
        | ((parameter == "wind_direction_10m") & ~value.between(0, 360))
        | (parameter.isin("precipitation", "wind_speed_10m") & (value < 0))
        | ((parameter == "pressure_msl") & (value <= 0))
    )
    errors.append(when(_finite_number(value) & out_of_range, lit("value_out_of_range")))
    for field, limit in _COORDINATE_LIMITS.items():
        coordinate = col(field)
        errors.append(
            when(
                coordinate.isNotNull()
                & (~_finite_number(coordinate) | ~coordinate.between(-limit, limit)),
                lit(f"invalid_{field}"),
            )
        )

    for field in _TIMESTAMP_COLUMNS:
        raw_timestamp = col(f"_weather_payload.{field}")
        errors.append(
            when(
                (trim(raw_timestamp) != "")
                & (~raw_timestamp.rlike(_UTC_TIMESTAMP_PATTERN) | col(field).isNull()),
                lit(f"invalid_{field}"),
            )
        )
    errors.extend(
        [
            when(
                unix_micros("measured_at_utc") % 3_600_000_000 != 0,
                lit("non_hourly_measured_at_utc"),
            ),
            when(
                col("measured_at_utc") > col("fetched_at_utc"),
                lit("future_measured_at_utc"),
            ),
        ]
    )
    return concat_ws(",", *errors)
