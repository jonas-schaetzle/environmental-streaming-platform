import argparse

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.streaming import StreamingQuery

from environmental_streaming.lakehouse.tables import (
    ICEBERG_WEATHER_QUARANTINE_TABLE,
    KAFKA_IDENTITY_COLUMNS,
    QUARANTINE_WEATHER_COLUMNS,
    WEATHER_QUARANTINE_BATCH_VIEW,
    WEATHER_QUARANTINE_CHECKPOINT_PATH,
    ensure_weather_quarantine_table,
)
from environmental_streaming.messaging.kafka_publisher import (
    KAFKA_BOOTSTRAP_SERVERS,
    WEATHER_KAFKA_TOPIC,
)
from environmental_streaming.processing.measurement_stream import (
    merge_iceberg_batch,
    read_kafka_stream,
    write_iceberg_stream,
)
from environmental_streaming.processing.weather_model import (
    filter_invalid_weather_observations,
    parse_weather_observations,
)
from environmental_streaming.runtime.spark import create_spark_session


def parse_kafka_weather(kafka_messages: DataFrame) -> DataFrame:
    payloads = kafka_messages.select(
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_value"),
    )
    return parse_weather_observations(payloads)


def merge_weather_quarantine_batch(weather: DataFrame, _batch_id: int) -> None:
    if weather.isEmpty():
        return
    merge_iceberg_batch(
        weather.select(*QUARANTINE_WEATHER_COLUMNS).dropDuplicates(
            list(KAFKA_IDENTITY_COLUMNS)
        ),
        ICEBERG_WEATHER_QUARANTINE_TABLE,
        WEATHER_QUARANTINE_BATCH_VIEW,
        QUARANTINE_WEATHER_COLUMNS,
        KAFKA_IDENTITY_COLUMNS,
    )


def write_weather_quarantine_stream(
    invalid_weather: DataFrame,
    checkpoint_path: str = WEATHER_QUARANTINE_CHECKPOINT_PATH,
) -> StreamingQuery:
    return write_iceberg_stream(
        invalid_weather, merge_weather_quarantine_batch, checkpoint_path
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Store invalid Kafka weather events in Iceberg."
    )
    parser.add_argument("--topic", default=WEATHER_KAFKA_TOPIC)
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--checkpoint-path", default=WEATHER_QUARANTINE_CHECKPOINT_PATH)
    args = parser.parse_args()
    spark = create_spark_session(app_name="weather-quarantine-stream")
    try:
        spark.sparkContext.setLogLevel("WARN")
        ensure_weather_quarantine_table(spark)
        parsed = parse_kafka_weather(
            read_kafka_stream(spark, args.topic, args.bootstrap_servers)
        )
        invalid = filter_invalid_weather_observations(parsed)
        write_weather_quarantine_stream(
            invalid, args.checkpoint_path
        ).awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
