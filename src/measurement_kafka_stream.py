from pathlib import Path

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


SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
ICEBERG_SPARK_PACKAGE = (
    "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
)
SPARK_PACKAGES = ",".join((SPARK_KAFKA_PACKAGE, ICEBERG_SPARK_PACKAGE))
LOCAL_SHUFFLE_PARTITIONS = "4"

ICEBERG_CATALOG = "local"
ICEBERG_NAMESPACE = f"{ICEBERG_CATALOG}.lake"
ICEBERG_CANONICAL_TABLE = f"{ICEBERG_NAMESPACE}.canonical_measurements"
ICEBERG_WAREHOUSE_PATH = str(
    Path(__file__).resolve().parent.parent / "data" / "warehouse"
)
ICEBERG_CANONICAL_BATCH_VIEW = "iceberg_canonical_measurement_batch"
ICEBERG_CANONICAL_CHECKPOINT_PATH = (
    "data/checkpoints/iceberg_canonical_measurements"
)

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
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse",
            ICEBERG_WAREHOUSE_PATH,
        )
        .getOrCreate()
    )


def ensure_iceberg_tables(spark: SparkSession) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_NAMESPACE}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_CANONICAL_TABLE} (
            kafka_topic STRING,
            kafka_partition INT,
            kafka_offset BIGINT,
            kafka_timestamp TIMESTAMP,
            kafka_key STRING,
            raw_value STRING,
            source STRING,
            location_id BIGINT,
            sensor_id BIGINT,
            parameter STRING,
            parameter_display_name STRING,
            value DOUBLE,
            unit STRING,
            measured_at_utc TIMESTAMP,
            latitude DOUBLE,
            longitude DOUBLE
        )
        USING iceberg
        PARTITIONED BY (days(measured_at_utc))
        TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet'
        )
        """
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


def merge_iceberg_measurement_batch(
    measurements: DataFrame,
    _batch_id: int,
) -> None:
    spark = measurements.sparkSession
    measurements.createOrReplaceTempView(ICEBERG_CANONICAL_BATCH_VIEW)

    try:
        spark.sql(
            f"""
            MERGE INTO {ICEBERG_CANONICAL_TABLE} AS target
            USING {ICEBERG_CANONICAL_BATCH_VIEW} AS incoming
            ON target.kafka_topic = incoming.kafka_topic
                AND target.kafka_partition = incoming.kafka_partition
                AND target.kafka_offset = incoming.kafka_offset
            WHEN NOT MATCHED THEN INSERT (
                kafka_topic,
                kafka_partition,
                kafka_offset,
                kafka_timestamp,
                kafka_key,
                raw_value,
                source,
                location_id,
                sensor_id,
                parameter,
                parameter_display_name,
                value,
                unit,
                measured_at_utc,
                latitude,
                longitude
            ) VALUES (
                incoming.kafka_topic,
                incoming.kafka_partition,
                incoming.kafka_offset,
                incoming.kafka_timestamp,
                incoming.kafka_key,
                incoming.raw_value,
                incoming.source,
                incoming.location_id,
                incoming.sensor_id,
                incoming.parameter,
                incoming.parameter_display_name,
                incoming.value,
                incoming.unit,
                incoming.measured_at_utc,
                incoming.latitude,
                incoming.longitude
            )
            """
        ).collect()
    finally:
        spark.catalog.dropTempView(ICEBERG_CANONICAL_BATCH_VIEW)


def write_iceberg_measurement_stream(measurements: DataFrame) -> StreamingQuery:
    return (
        measurements.writeStream.foreachBatch(merge_iceberg_measurement_batch)
        .outputMode("append")
        .option("checkpointLocation", ICEBERG_CANONICAL_CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start()
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
    ensure_iceberg_tables(spark)

    kafka_messages = read_kafka_stream(spark)
    parsed_measurements = parse_kafka_measurements(kafka_messages)
    normalized_measurements = normalize_measurement_units(parsed_measurements)
    valid_measurements = filter_valid_measurements(normalized_measurements)
    invalid_measurements = filter_invalid_measurements(parsed_measurements)
    hourly_aggregates = aggregate_measurements(valid_measurements)

    valid_query = write_kafka_measurement_stream(valid_measurements)
    iceberg_query = write_iceberg_measurement_stream(valid_measurements)
    invalid_query = write_invalid_kafka_measurement_stream(invalid_measurements)
    aggregate_query = write_hourly_aggregate_stream(hourly_aggregates)

    valid_query.awaitTermination()
    iceberg_query.awaitTermination()
    invalid_query.awaitTermination()
    aggregate_query.awaitTermination()

    spark.stop()


if __name__ == "__main__":
    main()
