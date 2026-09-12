from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws, lit, when
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


STREAM_INPUT_PATH = "data/stream/input"
STREAM_OUTPUT_PATH = "data/stream/output/canonical_measurements"
CHECKPOINT_PATH = "data/stream/checkpoints/canonical_measurements"
INVALID_STREAM_OUTPUT_PATH = "data/stream/output/invalid_measurements"
INVALID_CHECKPOINT_PATH = "data/stream/checkpoints/invalid_measurements"
LOCAL_SHUFFLE_PARTITIONS = "4"


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


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("measurement-file-stream")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", LOCAL_SHUFFLE_PARTITIONS)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_measurement_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream
        .schema(canonical_measurement_schema())
        .json(STREAM_INPUT_PATH)
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


def write_measurement_stream(
    measurements: DataFrame,
    output_path: str,
    checkpoint_path: str,
) -> StreamingQuery:
    return (
        measurements.writeStream
        .format("parquet")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start(output_path)
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    measurements = read_measurement_stream(spark)
    valid_measurements = filter_valid_measurements(measurements)
    invalid_measurements = filter_invalid_measurements(measurements)

    valid_query = write_measurement_stream(
        valid_measurements,
        output_path=STREAM_OUTPUT_PATH,
        checkpoint_path=CHECKPOINT_PATH,
    )
    invalid_query = write_measurement_stream(
        invalid_measurements,
        output_path=INVALID_STREAM_OUTPUT_PATH,
        checkpoint_path=INVALID_CHECKPOINT_PATH,
    )

    valid_query.awaitTermination()
    invalid_query.awaitTermination()

    spark.stop()


if __name__ == "__main__":
    main()
