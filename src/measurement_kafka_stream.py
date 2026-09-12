from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.streaming import StreamingQuery

if __package__:
    from .measurement_file_stream import (
        LOCAL_SHUFFLE_PARTITIONS,
        canonical_measurement_schema,
        filter_valid_measurements,
    )
else:
    from measurement_file_stream import (
        LOCAL_SHUFFLE_PARTITIONS,
        canonical_measurement_schema,
        filter_valid_measurements,
    )


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "environment.measurements.raw"
SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
KAFKA_STREAM_OUTPUT_PATH = "data/stream/output/kafka_canonical_measurements"
KAFKA_STREAM_CHECKPOINT_PATH = "data/stream/checkpoints/kafka_canonical_measurements"


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("measurement-kafka-stream")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", LOCAL_SHUFFLE_PARTITIONS)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", SPARK_KAFKA_PACKAGE)
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
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


def write_kafka_measurement_stream(measurements: DataFrame) -> StreamingQuery:
    return (
        measurements.writeStream
        .format("parquet")
        .outputMode("append")
        .option("checkpointLocation", KAFKA_STREAM_CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start(KAFKA_STREAM_OUTPUT_PATH)
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    kafka_messages = read_kafka_stream(spark)
    parsed_measurements = parse_kafka_measurements(kafka_messages)
    valid_measurements = filter_valid_measurements(parsed_measurements)

    query = write_kafka_measurement_stream(valid_measurements)
    query.awaitTermination()

    spark.stop()


if __name__ == "__main__":
    main()
