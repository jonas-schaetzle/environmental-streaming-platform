from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
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
    return measurements.filter(
        col("source").isNotNull()
        & col("location_id").isNotNull()
        & col("sensor_id").isNotNull()
        & col("parameter").isNotNull()
        & col("value").isNotNull()
        & (col("value") >= 0)
        & col("unit").isNotNull()
        & col("measured_at_utc").isNotNull()
    )


def write_measurement_stream(measurements: DataFrame) -> StreamingQuery:
    return (
        measurements.writeStream
        .format("parquet")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start(STREAM_OUTPUT_PATH)
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    measurements = filter_valid_measurements(read_measurement_stream(spark))
    query = write_measurement_stream(measurements)
    query.awaitTermination()

    spark.stop()


if __name__ == "__main__":
    main()
