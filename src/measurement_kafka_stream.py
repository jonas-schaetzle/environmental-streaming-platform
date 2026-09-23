from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    from_json,
    max as spark_max,
    min as spark_min,
    window,
)
from pyspark.sql.streaming import StreamingQuery

if __package__:
    from .kafka_publisher import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
    from .measurement_model import (
        canonical_measurement_schema,
        filter_invalid_measurements,
        filter_valid_measurements,
        normalize_measurement_units,
    )
    from .spark_runtime import configure_java_runtime
else:
    from kafka_publisher import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
    from measurement_model import (
        canonical_measurement_schema,
        filter_invalid_measurements,
        filter_valid_measurements,
        normalize_measurement_units,
    )
    from spark_runtime import configure_java_runtime


SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
LOCAL_SHUFFLE_PARTITIONS = "4"
KAFKA_STREAM_OUTPUT_PATH = "data/lake/canonical_measurements"
KAFKA_STREAM_CHECKPOINT_PATH = "data/checkpoints/canonical_measurements"
KAFKA_INVALID_STREAM_OUTPUT_PATH = "data/lake/quarantine_measurements"
KAFKA_INVALID_STREAM_CHECKPOINT_PATH = "data/checkpoints/quarantine_measurements"
HOURLY_AGGREGATE_OUTPUT_PATH = "data/lake/hourly_measurement_aggregates"
HOURLY_AGGREGATE_CHECKPOINT_PATH = "data/checkpoints/hourly_measurement_aggregates"
MEASUREMENT_WINDOW_DURATION = "1 hour"
MEASUREMENT_WATERMARK_DELAY = "2 hours"


def create_spark_session() -> SparkSession:
    configure_java_runtime()

    return (
        SparkSession.builder.appName("measurement-kafka-stream")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", LOCAL_SHUFFLE_PARTITIONS)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", SPARK_KAFKA_PACKAGE)
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )


def parse_kafka_measurements(kafka_messages: DataFrame) -> DataFrame:
    kafka_values = kafka_messages.select(
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_value"),
    )

    parsed_measurements = kafka_values.withColumn(
        "measurement",
        from_json(col("raw_value"), canonical_measurement_schema()),
    )

    return parsed_measurements.select(
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "kafka_key",
        "raw_value",
        "measurement.*",
    )


def aggregate_measurements(
    measurements: DataFrame,
    window_duration: str = MEASUREMENT_WINDOW_DURATION,
    watermark_delay: str = MEASUREMENT_WATERMARK_DELAY,
) -> DataFrame:
    return (
        measurements.withWatermark("measured_at_utc", watermark_delay)
        .groupBy(
            window(col("measured_at_utc"), window_duration),
            col("source"),
            col("location_id"),
            col("parameter"),
            col("unit"),
        )
        .agg(
            count("*").alias("measurement_count"),
            avg("value").alias("average_value"),
            spark_min("value").alias("minimum_value"),
            spark_max("value").alias("maximum_value"),
            spark_max("measured_at_utc").alias("latest_measured_at_utc"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "source",
            "location_id",
            "parameter",
            "unit",
            "measurement_count",
            "average_value",
            "minimum_value",
            "maximum_value",
            "latest_measured_at_utc",
        )
    )


def write_kafka_measurement_stream(measurements: DataFrame) -> StreamingQuery:
    return (
        measurements.writeStream.format("parquet")
        .outputMode("append")
        .option("checkpointLocation", KAFKA_STREAM_CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start(KAFKA_STREAM_OUTPUT_PATH)
    )


def write_invalid_kafka_measurement_stream(measurements: DataFrame) -> StreamingQuery:
    return (
        measurements.writeStream.format("parquet")
        .outputMode("append")
        .option("checkpointLocation", KAFKA_INVALID_STREAM_CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start(KAFKA_INVALID_STREAM_OUTPUT_PATH)
    )


def write_hourly_aggregate_stream(aggregates: DataFrame) -> StreamingQuery:
    return (
        aggregates.writeStream.format("parquet")
        .outputMode("append")
        .option("checkpointLocation", HOURLY_AGGREGATE_CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start(HOURLY_AGGREGATE_OUTPUT_PATH)
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    kafka_messages = read_kafka_stream(spark)
    parsed_measurements = parse_kafka_measurements(kafka_messages)
    normalized_measurements = normalize_measurement_units(parsed_measurements)
    valid_measurements = filter_valid_measurements(normalized_measurements)
    invalid_measurements = filter_invalid_measurements(parsed_measurements)
    hourly_aggregates = aggregate_measurements(valid_measurements)

    valid_query = write_kafka_measurement_stream(valid_measurements)
    invalid_query = write_invalid_kafka_measurement_stream(invalid_measurements)
    aggregate_query = write_hourly_aggregate_stream(hourly_aggregates)

    valid_query.awaitTermination()
    invalid_query.awaitTermination()
    aggregate_query.awaitTermination()

    spark.stop()


if __name__ == "__main__":
    main()
