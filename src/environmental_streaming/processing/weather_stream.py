import argparse
from functools import reduce
from operator import and_

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, countDistinct, row_number, struct
from pyspark.sql.streaming import StreamingQuery

from environmental_streaming.lakehouse.tables import (
    CANONICAL_WEATHER_COLUMNS,
    ICEBERG_WEATHER_CANONICAL_TABLE,
    ICEBERG_WEATHER_QUARANTINE_TABLE,
    KAFKA_IDENTITY_COLUMNS,
    QUARANTINE_WEATHER_COLUMNS,
    WEATHER_CANONICAL_BATCH_VIEW,
    WEATHER_CANONICAL_CHECKPOINT_PATH,
    WEATHER_QUARANTINE_BATCH_VIEW,
    WEATHER_QUARANTINE_CHECKPOINT_PATH,
    ensure_weather_canonical_table,
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
    WEATHER_IDENTITY_COLUMNS,
    canonical_weather_schema,
    filter_invalid_weather_observations,
    filter_valid_weather_observations,
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


def prepare_weather_canonical_batch(weather: DataFrame) -> DataFrame:
    payload_columns = canonical_weather_schema().fieldNames()
    conflicts = (
        weather.groupBy(*WEATHER_IDENTITY_COLUMNS, "fetched_at_utc")
        .agg(countDistinct(struct(*payload_columns)).alias("_versions"))
        .filter(col("_versions") > 1)
    )
    if conflicts.limit(1).count():
        raise ValueError(
            "Conflicting weather content for the same business key and receipt time."
        )
    ordering = Window.partitionBy(*WEATHER_IDENTITY_COLUMNS).orderBy(
        col("fetched_at_utc").desc(),
        col("kafka_topic").asc(),
        col("kafka_partition").asc(),
        col("kafka_offset").asc(),
    )
    return (
        weather.withColumn("_revision_rank", row_number().over(ordering))
        .filter(col("_revision_rank") == 1)
        .select(*CANONICAL_WEATHER_COLUMNS)
    )


def merge_weather_canonical_batch(weather: DataFrame, _batch_id: int) -> None:
    batch = weather.select(*CANONICAL_WEATHER_COLUMNS).persist()
    spark = weather.sparkSession
    try:
        if batch.isEmpty():
            return
        newest = prepare_weather_canonical_batch(batch)
        spark.catalog.refreshTable(ICEBERG_WEATHER_CANONICAL_TABLE)
        target = spark.table(ICEBERG_WEATHER_CANONICAL_TABLE).alias("target")
        same_revision = reduce(
            and_,
            (
                col(f"incoming.{field}") == col(f"target.{field}")
                for field in (*WEATHER_IDENTITY_COLUMNS, "fetched_at_utc")
            ),
        )
        fields = canonical_weather_schema().fieldNames()
        identical = struct(*[col(f"incoming.{field}") for field in fields]).eqNullSafe(
            struct(*[col(f"target.{field}") for field in fields])
        )
        conflicts = (
            batch.alias("incoming").join(target, same_revision).filter(~identical)
        )
        if conflicts.limit(1).count():
            raise ValueError(
                "Weather receipt conflicts with the currently stored revision."
            )
        newest.createOrReplaceTempView(WEATHER_CANONICAL_BATCH_VIEW)
        condition = " AND ".join(
            f"target.{field} = incoming.{field}" for field in WEATHER_IDENTITY_COLUMNS
        )
        assignments = ", ".join(
            f"target.{field} = incoming.{field}" for field in CANONICAL_WEATHER_COLUMNS
        )
        columns = ", ".join(CANONICAL_WEATHER_COLUMNS)
        values = ", ".join(f"incoming.{field}" for field in CANONICAL_WEATHER_COLUMNS)
        spark.sql(f"""
            MERGE INTO {ICEBERG_WEATHER_CANONICAL_TABLE} AS target
            USING {WEATHER_CANONICAL_BATCH_VIEW} AS incoming
            ON {condition}
            WHEN MATCHED AND incoming.fetched_at_utc > target.fetched_at_utc
                THEN UPDATE SET {assignments}
            WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values})
        """).collect()
    finally:
        spark.catalog.dropTempView(WEATHER_CANONICAL_BATCH_VIEW)
        batch.unpersist()


def write_weather_canonical_stream(
    valid_weather: DataFrame,
    checkpoint_path: str = WEATHER_CANONICAL_CHECKPOINT_PATH,
) -> StreamingQuery:
    return write_iceberg_stream(
        valid_weather, merge_weather_canonical_batch, checkpoint_path
    )


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
        description="Store canonical or quarantined Kafka weather events in Iceberg."
    )
    parser.add_argument(
        "--sink", choices=("quarantine", "canonical"), default="quarantine"
    )
    parser.add_argument("--topic", default=WEATHER_KAFKA_TOPIC)
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--checkpoint-path")
    args = parser.parse_args()
    spark = create_spark_session(app_name=f"weather-{args.sink}-stream")
    try:
        spark.sparkContext.setLogLevel("WARN")
        if args.sink == "canonical":
            ensure_weather_canonical_table(spark)
        else:
            ensure_weather_quarantine_table(spark)
        parsed = parse_kafka_weather(
            read_kafka_stream(spark, args.topic, args.bootstrap_servers)
        )
        if args.sink == "canonical":
            valid = filter_valid_weather_observations(parsed)
            query = write_weather_canonical_stream(
                valid, args.checkpoint_path or WEATHER_CANONICAL_CHECKPOINT_PATH
            )
        else:
            invalid = filter_invalid_weather_observations(parsed)
            query = write_weather_quarantine_stream(
                invalid, args.checkpoint_path or WEATHER_QUARANTINE_CHECKPOINT_PATH
            )
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
