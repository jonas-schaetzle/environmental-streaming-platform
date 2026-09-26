import json
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BinaryType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.measurement_model import filter_invalid_measurements
from src.measurement_kafka_stream import (
    aggregate_measurements,
    ensure_iceberg_tables,
    merge_iceberg_hourly_aggregate_batch,
    merge_iceberg_measurement_batch,
    merge_iceberg_quarantine_batch,
    parse_kafka_measurements,
)


def test_ensure_iceberg_tables_creates_expected_tables() -> None:
    spark = MagicMock(spec=SparkSession)

    ensure_iceberg_tables(spark)

    namespace_sql = spark.sql.call_args_list[0].args[0]
    table_sql = spark.sql.call_args_list[1].args[0]
    quarantine_table_sql = spark.sql.call_args_list[2].args[0]
    aggregate_table_sql = spark.sql.call_args_list[3].args[0]

    assert namespace_sql == "CREATE NAMESPACE IF NOT EXISTS local.lake"
    assert "CREATE TABLE IF NOT EXISTS local.lake.canonical_measurements" in table_sql
    assert "PARTITIONED BY (days(measured_at_utc))" in table_sql
    assert "'format-version' = '2'" in table_sql
    assert (
        "CREATE TABLE IF NOT EXISTS local.lake.quarantine_measurements"
        in quarantine_table_sql
    )
    assert "validation_error STRING" in quarantine_table_sql
    assert "PARTITIONED BY (days(kafka_timestamp))" in quarantine_table_sql
    assert (
        "CREATE TABLE IF NOT EXISTS local.lake.hourly_measurement_aggregates"
        in aggregate_table_sql
    )
    assert "measurement_count BIGINT" in aggregate_table_sql
    assert "PARTITIONED BY (days(window_start))" in aggregate_table_sql


def test_merge_iceberg_measurement_batch_inserts_only_unknown_kafka_offsets() -> None:
    spark = MagicMock(spec=SparkSession)
    merge_result = spark.sql.return_value
    measurements = MagicMock(spec=DataFrame)
    measurements.sparkSession = spark

    merge_iceberg_measurement_batch(measurements, 7)

    measurements.createOrReplaceTempView.assert_called_once_with(
        "iceberg_canonical_measurement_batch"
    )
    merge_sql = spark.sql.call_args.args[0]
    assert "MERGE INTO local.lake.canonical_measurements AS target" in merge_sql
    assert "target.kafka_topic = incoming.kafka_topic" in merge_sql
    assert "target.kafka_partition = incoming.kafka_partition" in merge_sql
    assert "target.kafka_offset = incoming.kafka_offset" in merge_sql
    assert "WHEN NOT MATCHED THEN INSERT" in merge_sql
    assert "WHEN MATCHED" not in merge_sql
    merge_result.collect.assert_called_once_with()
    spark.catalog.dropTempView.assert_called_once_with(
        "iceberg_canonical_measurement_batch"
    )


def test_merge_iceberg_quarantine_batch_preserves_validation_error() -> None:
    spark = MagicMock(spec=SparkSession)
    merge_result = spark.sql.return_value
    measurements = MagicMock(spec=DataFrame)
    measurements.sparkSession = spark

    merge_iceberg_quarantine_batch(measurements, 8)

    measurements.createOrReplaceTempView.assert_called_once_with(
        "iceberg_quarantine_measurement_batch"
    )
    merge_sql = spark.sql.call_args.args[0]
    assert "MERGE INTO local.lake.quarantine_measurements AS target" in merge_sql
    assert "target.kafka_topic = incoming.kafka_topic" in merge_sql
    assert "target.kafka_partition = incoming.kafka_partition" in merge_sql
    assert "target.kafka_offset = incoming.kafka_offset" in merge_sql
    assert "validation_error" in merge_sql
    assert "incoming.validation_error" in merge_sql
    merge_result.collect.assert_called_once_with()
    spark.catalog.dropTempView.assert_called_once_with(
        "iceberg_quarantine_measurement_batch"
    )


def test_merge_iceberg_hourly_aggregate_batch_updates_existing_window() -> None:
    spark = MagicMock(spec=SparkSession)
    merge_result = spark.sql.return_value
    aggregates = MagicMock(spec=DataFrame)
    aggregates.sparkSession = spark

    merge_iceberg_hourly_aggregate_batch(aggregates, 9)

    aggregates.createOrReplaceTempView.assert_called_once_with(
        "iceberg_hourly_measurement_aggregate_batch"
    )
    merge_sql = spark.sql.call_args.args[0]
    assert "MERGE INTO local.lake.hourly_measurement_aggregates AS target" in merge_sql
    assert "target.window_start = incoming.window_start" in merge_sql
    assert "target.window_end = incoming.window_end" in merge_sql
    assert "target.source = incoming.source" in merge_sql
    assert "target.location_id = incoming.location_id" in merge_sql
    assert "target.parameter = incoming.parameter" in merge_sql
    assert "target.unit = incoming.unit" in merge_sql
    assert "WHEN MATCHED THEN UPDATE SET" in merge_sql
    assert "target.measurement_count = incoming.measurement_count" in merge_sql
    assert "target.average_value = incoming.average_value" in merge_sql
    assert "WHEN NOT MATCHED THEN INSERT" in merge_sql
    merge_result.collect.assert_called_once_with()
    spark.catalog.dropTempView.assert_called_once_with(
        "iceberg_hourly_measurement_aggregate_batch"
    )


def test_parse_kafka_measurements_extracts_metadata_and_measurement(
    spark: SparkSession,
) -> None:
    kafka_schema = StructType(
        [
            StructField("topic", StringType(), nullable=False),
            StructField("partition", LongType(), nullable=False),
            StructField("offset", LongType(), nullable=False),
            StructField("timestamp", StringType(), nullable=False),
            StructField("key", BinaryType(), nullable=False),
            StructField("value", BinaryType(), nullable=False),
        ]
    )
    kafka_messages = spark.createDataFrame(
        [
            (
                "environment.measurements.canonical",
                2,
                0,
                "2026-08-13 15:00:01",
                b"3916",
                (
                    b'{"source":"openaq","location_id":2178,"sensor_id":3916,'
                    b'"parameter":"no2","parameter_display_name":"NO2",'
                    b'"value":0.007,"unit":"ppm",'
                    b'"measured_at_utc":"2026-08-13T15:00:00Z",'
                    b'"latitude":35.1353,"longitude":-106.584702}'
                ),
            )
        ],
        kafka_schema,
    )

    result = parse_kafka_measurements(kafka_messages)

    row = result.collect()[0]

    assert row["kafka_topic"] == "environment.measurements.canonical"
    assert row["kafka_partition"] == 2
    assert row["kafka_offset"] == 0
    assert row["kafka_key"] == "3916"
    assert row["source"] == "openaq"
    assert row["location_id"] == 2178
    assert row["sensor_id"] == 3916
    assert row["parameter"] == "no2"
    assert row["value"] == 0.007


def test_invalid_kafka_measurements_keep_metadata_and_error_reason(
    spark: SparkSession,
) -> None:
    kafka_schema = StructType(
        [
            StructField("topic", StringType(), nullable=False),
            StructField("partition", LongType(), nullable=False),
            StructField("offset", LongType(), nullable=False),
            StructField("timestamp", StringType(), nullable=False),
            StructField("key", BinaryType(), nullable=False),
            StructField("value", BinaryType(), nullable=False),
        ]
    )
    kafka_messages = spark.createDataFrame(
        [
            (
                "environment.measurements.canonical",
                1,
                14,
                "2026-09-10 14:00:01",
                b"9999",
                (
                    b'{"source":"openaq","location_id":2178,"sensor_id":9999,'
                    b'"parameter":"pm25","value":-1.0,'
                    b'"measured_at_utc":"2026-09-10T14:00:00Z"}'
                ),
            )
        ],
        kafka_schema,
    )

    parsed_measurements = parse_kafka_measurements(kafka_messages)
    invalid_measurements = filter_invalid_measurements(parsed_measurements)

    row = invalid_measurements.collect()[0]

    assert row["kafka_topic"] == "environment.measurements.canonical"
    assert row["kafka_partition"] == 1
    assert row["kafka_offset"] == 14
    assert row["kafka_key"] == "9999"
    assert row["sensor_id"] == 9999
    assert row["validation_error"] == "negative_value,missing_unit"
    assert '"value":-1.0' in row["raw_value"]


def test_malformed_kafka_json_becomes_invalid_measurement(
    spark: SparkSession,
) -> None:
    kafka_schema = StructType(
        [
            StructField("topic", StringType(), nullable=False),
            StructField("partition", LongType(), nullable=False),
            StructField("offset", LongType(), nullable=False),
            StructField("timestamp", StringType(), nullable=False),
            StructField("key", BinaryType(), nullable=False),
            StructField("value", BinaryType(), nullable=False),
        ]
    )
    kafka_messages = spark.createDataFrame(
        [
            (
                "environment.measurements.canonical",
                1,
                15,
                "2026-09-10 14:00:02",
                b"bad-json",
                b"not-json",
            )
        ],
        kafka_schema,
    )

    parsed_measurements = parse_kafka_measurements(kafka_messages)
    invalid_measurements = filter_invalid_measurements(parsed_measurements)

    row = invalid_measurements.collect()[0]

    assert row["kafka_offset"] == 15
    assert row["kafka_key"] == "bad-json"
    assert row["raw_value"] == "not-json"
    assert row["validation_error"] == (
        "missing_source,missing_location_id,missing_sensor_id,"
        "missing_parameter,missing_value,missing_unit,missing_measured_at_utc"
    )


def test_aggregate_measurements_calculates_hourly_location_statistics(
    spark: SparkSession,
) -> None:
    schema = StructType(
        [
            StructField("source", StringType(), nullable=False),
            StructField("location_id", LongType(), nullable=False),
            StructField("parameter", StringType(), nullable=False),
            StructField("unit", StringType(), nullable=False),
            StructField("value", DoubleType(), nullable=False),
            StructField("measured_at_utc", TimestampType(), nullable=False),
        ]
    )
    measurements = spark.createDataFrame(
        [
            (
                "openaq",
                2669,
                "pm25",
                "µg/m³",
                10.0,
                datetime(2026, 9, 21, 13, 5),
            ),
            (
                "openaq",
                2669,
                "pm25",
                "µg/m³",
                14.0,
                datetime(2026, 9, 21, 13, 45),
            ),
            (
                "openaq",
                2936,
                "pm25",
                "µg/m³",
                8.0,
                datetime(2026, 9, 21, 13, 30),
            ),
            (
                "another-source",
                2669,
                "pm25",
                "µg/m³",
                100.0,
                datetime(2026, 9, 21, 13, 30),
            ),
        ],
        schema,
    )

    rows = {
        (row["source"], row["location_id"]): row
        for row in aggregate_measurements(measurements).collect()
    }

    munich_row = rows[("openaq", 2669)]
    assert munich_row["window_start"] == datetime(2026, 9, 21, 13, 0)
    assert munich_row["window_end"] == datetime(2026, 9, 21, 14, 0)
    assert munich_row["measurement_count"] == 2
    assert munich_row["average_value"] == 12.0
    assert munich_row["minimum_value"] == 10.0
    assert munich_row["maximum_value"] == 14.0
    assert munich_row["latest_measured_at_utc"] == datetime(
        2026,
        9,
        21,
        13,
        45,
    )
    assert rows[("openaq", 2936)]["measurement_count"] == 1
    assert rows[("another-source", 2669)]["average_value"] == 100.0


def test_event_time_watermark_finalizes_windows_and_drops_late_events(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "input"
    checkpoint_path = tmp_path / "checkpoint"
    input_path.mkdir()
    query_name = f"hourly_aggregates_{uuid4().hex}"
    schema = StructType(
        [
            StructField("source", StringType(), nullable=False),
            StructField("location_id", LongType(), nullable=False),
            StructField("parameter", StringType(), nullable=False),
            StructField("unit", StringType(), nullable=False),
            StructField("value", DoubleType(), nullable=False),
            StructField("measured_at_utc", TimestampType(), nullable=False),
        ]
    )
    measurements = (
        spark.readStream.schema(schema)
        .option("maxFilesPerTrigger", 1)
        .json(str(input_path))
    )
    aggregates = aggregate_measurements(
        measurements,
        watermark_delay="2 hours",
    )
    query = (
        aggregates.writeStream.format("memory")
        .queryName(query_name)
        .outputMode("append")
        .option("checkpointLocation", str(checkpoint_path))
        .start()
    )

    def write_batch(file_name: str, events: list[dict[str, object]]) -> None:
        content = "\n".join(json.dumps(event) for event in events) + "\n"
        (input_path / file_name).write_text(content, encoding="utf-8")
        query.processAllAvailable()

    def measurement_event(
        location_id: int,
        value: float,
        measured_at_utc: str,
    ) -> dict[str, object]:
        return {
            "source": "openaq",
            "location_id": location_id,
            "parameter": "pm25",
            "unit": "ug/m3",
            "value": value,
            "measured_at_utc": measured_at_utc,
        }

    try:
        write_batch(
            "batch-1.jsonl",
            [
                measurement_event(2669, 10.0, "2026-09-21T10:10:00Z"),
                measurement_event(2669, 20.0, "2026-09-21T10:40:00Z"),
            ],
        )
        write_batch(
            "batch-2.jsonl",
            [measurement_event(2936, 8.0, "2026-09-21T13:05:00Z")],
        )
        write_batch(
            "batch-3.jsonl",
            [measurement_event(3071, 12.0, "2026-09-21T14:05:00Z")],
        )

        finalized_rows = spark.table(query_name).collect()
        assert len(finalized_rows) == 1
        assert finalized_rows[0]["source"] == "openaq"
        assert finalized_rows[0]["location_id"] == 2669
        assert finalized_rows[0]["measurement_count"] == 2
        assert finalized_rows[0]["average_value"] == 15.0

        write_batch(
            "batch-4-late.jsonl",
            [measurement_event(2669, 30.0, "2026-09-21T10:30:00Z")],
        )

        rows_after_late_event = spark.table(query_name).collect()
        assert len(rows_after_late_event) == 1
        assert rows_after_late_event[0]["measurement_count"] == 2
        assert rows_after_late_event[0]["average_value"] == 15.0
    finally:
        query.stop()
