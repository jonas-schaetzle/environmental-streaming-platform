from collections.abc import Callable

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

from environmental_streaming.lakehouse.tables import (
    CANONICAL_MEASUREMENT_COLUMNS,
    HOURLY_AGGREGATE_COLUMNS,
    HOURLY_AGGREGATE_IDENTITY_COLUMNS,
    HOURLY_AGGREGATE_UPDATE_COLUMNS,
    ICEBERG_CANONICAL_BATCH_VIEW,
    ICEBERG_CANONICAL_CHECKPOINT_PATH,
    ICEBERG_CANONICAL_TABLE,
    ICEBERG_HOURLY_AGGREGATE_BATCH_VIEW,
    ICEBERG_HOURLY_AGGREGATE_CHECKPOINT_PATH,
    ICEBERG_HOURLY_AGGREGATE_TABLE,
    ICEBERG_QUARANTINE_BATCH_VIEW,
    ICEBERG_QUARANTINE_CHECKPOINT_PATH,
    ICEBERG_QUARANTINE_TABLE,
    KAFKA_IDENTITY_COLUMNS,
    QUARANTINE_MEASUREMENT_COLUMNS,
    ensure_iceberg_tables,
)
from environmental_streaming.messaging.kafka_publisher import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
)
from environmental_streaming.processing.measurement_model import (
    canonical_measurement_schema,
    filter_invalid_measurements,
    filter_valid_measurements,
    normalize_measurement_units,
)
from environmental_streaming.runtime.spark import create_spark_session

MEASUREMENT_WINDOW_DURATION = "1 hour"
MEASUREMENT_WATERMARK_DELAY = "2 hours"


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


def _merge_iceberg_batch(
    measurements: DataFrame,
    target_table: str,
    batch_view: str,
    columns: tuple[str, ...],
    identity_columns: tuple[str, ...],
    update_columns: tuple[str, ...] = (),
) -> None:
    spark = measurements.sparkSession
    merge_condition = "\n                AND ".join(
        f"target.{column} = incoming.{column}" for column in identity_columns
    )
    insert_columns = ",\n                ".join(columns)
    incoming_columns = ",\n                ".join(
        f"incoming.{column}" for column in columns
    )
    update_assignments = ",\n                ".join(
        f"target.{column} = incoming.{column}" for column in update_columns
    )
    update_clause = (
        f"""
            WHEN MATCHED THEN UPDATE SET
                {update_assignments}
        """
        if update_columns
        else ""
    )
    measurements.createOrReplaceTempView(batch_view)

    try:
        spark.sql(
            f"""
            MERGE INTO {target_table} AS target
            USING {batch_view} AS incoming
            ON {merge_condition}
            {update_clause}
            WHEN NOT MATCHED THEN INSERT (
                {insert_columns}
            ) VALUES (
                {incoming_columns}
            )
            """
        ).collect()
    finally:
        spark.catalog.dropTempView(batch_view)


def merge_iceberg_measurement_batch(
    measurements: DataFrame,
    _batch_id: int,
) -> None:
    _merge_iceberg_batch(
        measurements,
        ICEBERG_CANONICAL_TABLE,
        ICEBERG_CANONICAL_BATCH_VIEW,
        CANONICAL_MEASUREMENT_COLUMNS,
        KAFKA_IDENTITY_COLUMNS,
    )


def merge_iceberg_quarantine_batch(
    measurements: DataFrame,
    _batch_id: int,
) -> None:
    _merge_iceberg_batch(
        measurements,
        ICEBERG_QUARANTINE_TABLE,
        ICEBERG_QUARANTINE_BATCH_VIEW,
        QUARANTINE_MEASUREMENT_COLUMNS,
        KAFKA_IDENTITY_COLUMNS,
    )


def merge_iceberg_hourly_aggregate_batch(
    aggregates: DataFrame,
    _batch_id: int,
) -> None:
    _merge_iceberg_batch(
        aggregates,
        ICEBERG_HOURLY_AGGREGATE_TABLE,
        ICEBERG_HOURLY_AGGREGATE_BATCH_VIEW,
        HOURLY_AGGREGATE_COLUMNS,
        HOURLY_AGGREGATE_IDENTITY_COLUMNS,
        HOURLY_AGGREGATE_UPDATE_COLUMNS,
    )


def _write_iceberg_stream(
    measurements: DataFrame,
    merge_batch: Callable[[DataFrame, int], None],
    checkpoint_path: str,
) -> StreamingQuery:
    return (
        measurements.writeStream.foreachBatch(merge_batch)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start()
    )


def write_iceberg_measurement_stream(measurements: DataFrame) -> StreamingQuery:
    return _write_iceberg_stream(
        measurements,
        merge_iceberg_measurement_batch,
        ICEBERG_CANONICAL_CHECKPOINT_PATH,
    )


def write_iceberg_quarantine_stream(measurements: DataFrame) -> StreamingQuery:
    return _write_iceberg_stream(
        measurements,
        merge_iceberg_quarantine_batch,
        ICEBERG_QUARANTINE_CHECKPOINT_PATH,
    )


def write_iceberg_hourly_aggregate_stream(
    aggregates: DataFrame,
) -> StreamingQuery:
    return _write_iceberg_stream(
        aggregates,
        merge_iceberg_hourly_aggregate_batch,
        ICEBERG_HOURLY_AGGREGATE_CHECKPOINT_PATH,
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

    canonical_query = write_iceberg_measurement_stream(valid_measurements)
    quarantine_query = write_iceberg_quarantine_stream(invalid_measurements)
    aggregate_query = write_iceberg_hourly_aggregate_stream(hourly_aggregates)

    canonical_query.awaitTermination()
    quarantine_query.awaitTermination()
    aggregate_query.awaitTermination()

    spark.stop()


if __name__ == "__main__":
    main()
