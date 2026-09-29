from pyspark.sql import SparkSession


ICEBERG_CATALOG = "local"
ICEBERG_NAMESPACE = f"{ICEBERG_CATALOG}.lake"
ICEBERG_CANONICAL_TABLE = f"{ICEBERG_NAMESPACE}.canonical_measurements"
ICEBERG_QUARANTINE_TABLE = f"{ICEBERG_NAMESPACE}.quarantine_measurements"
ICEBERG_HOURLY_AGGREGATE_TABLE = (
    f"{ICEBERG_NAMESPACE}.hourly_measurement_aggregates"
)

ICEBERG_CANONICAL_BATCH_VIEW = "iceberg_canonical_measurement_batch"
ICEBERG_CANONICAL_CHECKPOINT_PATH = (
    "data/checkpoints/iceberg_canonical_measurements"
)
ICEBERG_QUARANTINE_BATCH_VIEW = "iceberg_quarantine_measurement_batch"
ICEBERG_QUARANTINE_CHECKPOINT_PATH = (
    "data/checkpoints/iceberg_quarantine_measurements"
)
ICEBERG_HOURLY_AGGREGATE_BATCH_VIEW = "iceberg_hourly_measurement_aggregate_batch"
ICEBERG_HOURLY_AGGREGATE_CHECKPOINT_PATH = (
    "data/checkpoints/iceberg_hourly_measurement_aggregates"
)

CANONICAL_MEASUREMENT_COLUMNS = (
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "kafka_key",
    "raw_value",
    "source",
    "location_id",
    "sensor_id",
    "parameter",
    "parameter_display_name",
    "value",
    "unit",
    "measured_at_utc",
    "latitude",
    "longitude",
)
QUARANTINE_MEASUREMENT_COLUMNS = (
    *CANONICAL_MEASUREMENT_COLUMNS,
    "validation_error",
)
KAFKA_IDENTITY_COLUMNS = (
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
)
HOURLY_AGGREGATE_COLUMNS = (
    "window_start",
    "window_end",
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
HOURLY_AGGREGATE_IDENTITY_COLUMNS = (
    "window_start",
    "window_end",
    "source",
    "location_id",
    "parameter",
    "unit",
)
HOURLY_AGGREGATE_UPDATE_COLUMNS = (
    "measurement_count",
    "average_value",
    "minimum_value",
    "maximum_value",
    "latest_measured_at_utc",
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
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_QUARANTINE_TABLE} (
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
            longitude DOUBLE,
            validation_error STRING
        )
        USING iceberg
        PARTITIONED BY (days(kafka_timestamp))
        TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet'
        )
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_HOURLY_AGGREGATE_TABLE} (
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            source STRING,
            location_id BIGINT,
            parameter STRING,
            unit STRING,
            measurement_count BIGINT,
            average_value DOUBLE,
            minimum_value DOUBLE,
            maximum_value DOUBLE,
            latest_measured_at_utc TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(window_start))
        TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet'
        )
        """
    )
