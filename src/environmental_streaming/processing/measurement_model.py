from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col, concat_ws, lit, when
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def canonical_measurement_schema() -> StructType:
    return StructType(
        [
            StructField("source", StringType(), nullable=False),
            StructField("location_id", LongType(), nullable=False),
            StructField("sensor_id", LongType(), nullable=False),
            StructField("parameter", StringType(), nullable=False),
            StructField("parameter_display_name", StringType(), nullable=True),
            StructField("value", DoubleType(), nullable=False),
            StructField("unit", StringType(), nullable=False),
            StructField("measured_at_utc", TimestampType(), nullable=False),
            StructField("latitude", DoubleType(), nullable=True),
            StructField("longitude", DoubleType(), nullable=True),
        ]
    )


def filter_valid_measurements(measurements: DataFrame) -> DataFrame:
    return measurements.filter(valid_measurement_condition())


def filter_invalid_measurements(measurements: DataFrame) -> DataFrame:
    return measurements.filter(~valid_measurement_condition()).withColumn(
        "validation_error",
        concat_ws(
            ",",
            when(col("source").isNull(), lit("missing_source")),
            when(col("location_id").isNull(), lit("missing_location_id")),
            when(col("sensor_id").isNull(), lit("missing_sensor_id")),
            when(col("parameter").isNull(), lit("missing_parameter")),
            when(col("value").isNull(), lit("missing_value")),
            when(col("value") < 0, lit("negative_value")),
            when(col("unit").isNull(), lit("missing_unit")),
            when(col("measured_at_utc").isNull(), lit("missing_measured_at_utc")),
        ),
    )


def normalize_measurement_units(measurements: DataFrame) -> DataFrame:
    return measurements.withColumn(
        "unit",
        when(col("unit").isin("ug/m3", "µg/m³"), lit("µg/m³")).otherwise(
            col("unit")
        ),
    )


def valid_measurement_condition() -> Column:
    return (
        col("source").isNotNull()
        & col("location_id").isNotNull()
        & col("sensor_id").isNotNull()
        & col("parameter").isNotNull()
        & col("value").isNotNull()
        & (col("value") >= 0)
        & col("unit").isNotNull()
        & col("measured_at_utc").isNotNull()
    )
