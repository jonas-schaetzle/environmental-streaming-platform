import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, Mock, call

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import unix_micros
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from environmental_streaming.lakehouse.tables import (
    KAFKA_IDENTITY_COLUMNS,
    QUARANTINE_WEATHER_COLUMNS,
    WEATHER_QUARANTINE_CHECKPOINT_PATH,
    ensure_weather_quarantine_table,
)
from environmental_streaming.processing import weather_stream
from environmental_streaming.processing.measurement_stream import read_kafka_stream
from environmental_streaming.processing.weather_model import (
    canonical_weather_schema,
    filter_invalid_weather_observations,
)


def test_weather_quarantine_table_matches_contract_and_is_created_separately() -> None:
    spark = MagicMock(spec=SparkSession)
    ensure_weather_quarantine_table(spark)
    assert spark.sql.call_count == 2
    assert (
        spark.sql.call_args_list[0].args[0]
        == "CREATE NAMESPACE IF NOT EXISTS local.lake"
    )
    ddl = spark.sql.call_args.args[0]
    assert (
        "CREATE TABLE IF NOT EXISTS local.lake.quarantine_weather_observations" in ddl
    )
    assert "PARTITIONED BY (days(kafka_timestamp))" in ddl
    assert "'format-version' = '2'" in ddl
    assert "'write.format.default' = 'parquet'" in ddl
    assert QUARANTINE_WEATHER_COLUMNS == (
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "kafka_key",
        "raw_value",
        *canonical_weather_schema().fieldNames(),
        "validation_error",
    )
    for field in canonical_weather_schema():
        assert f"{field.name} {field.dataType.simpleString().upper()}" in ddl


def test_kafka_weather_routes_invalid_records_and_keeps_transport_evidence(
    spark: SparkSession,
) -> None:
    payload = {
        "source": "open_meteo",
        "source_dataset": "forecast_best_match",
        "weather_location_id": "munich",
        "city": "Munich",
        "latitude": 48.13743,
        "longitude": 11.57549,
        "grid_latitude": 48.14,
        "grid_longitude": 11.58,
        "parameter": "temperature_2m",
        "value": -5.0,
        "unit": "degC",
        "measured_at_utc": "2026-10-16T10:00:00Z",
        "fetched_at_utc": "2026-10-16T10:15:00Z",
    }
    raw = [
        json.dumps(payload),
        json.dumps({**payload, "value": None}),
        json.dumps(
            {
                **payload,
                "parameter": "relative_humidity_2m",
                "unit": "%",
                "value": 101.0,
            }
        ),
        '{"source":',
        None,
    ]
    schema = StructType(
        [
            StructField("topic", StringType()),
            StructField("partition", IntegerType()),
            StructField("offset", LongType()),
            StructField("timestamp", TimestampType()),
            StructField("key", BinaryType()),
            StructField("value", BinaryType()),
        ]
    )
    timestamp = datetime(2026, 10, 16, 10, 16, tzinfo=timezone.utc)
    frame = spark.createDataFrame(
        [
            (
                "environment.weather.canonical",
                2,
                offset,
                timestamp,
                b"open_meteo:munich" if value is not None else None,
                value.encode() if value is not None else None,
            )
            for offset, value in enumerate(raw)
        ],
        schema,
    )
    parsed = weather_stream.parse_kafka_weather(frame)
    assert set(parsed.columns) == set(QUARANTINE_WEATHER_COLUMNS)
    rows = (
        filter_invalid_weather_observations(parsed)
        .withColumn("kafka_micros", unix_micros("kafka_timestamp"))
        .collect()
    )
    assert {row.kafka_offset for row in rows} == {1, 2, 3, 4}
    for row in rows:
        assert (
            row.kafka_topic == "environment.weather.canonical"
            and row.kafka_partition == 2
        )
        assert row.raw_value == raw[row.kafka_offset]
        assert row.kafka_key == ("open_meteo:munich" if row.kafka_offset != 4 else None)
        assert row.kafka_micros == int(timestamp.timestamp()) * 10**6
        assert {
            1: "missing_value",
            2: "value_out_of_range",
            3: "invalid_payload",
            4: "invalid_payload",
        }[row.kafka_offset] in row.validation_error.split(",")


def test_quarantine_merge_uses_transport_identity_and_cleans_up_on_failure() -> None:
    spark = MagicMock(spec=SparkSession)
    batch = MagicMock(spec=DataFrame)
    batch.isEmpty.return_value = False
    incoming = batch.select.return_value.dropDuplicates.return_value
    incoming.sparkSession = spark
    weather_stream.merge_weather_quarantine_batch(batch, 1)
    batch.select.assert_called_once_with(*QUARANTINE_WEATHER_COLUMNS)
    batch.select.return_value.dropDuplicates.assert_called_once_with(
        list(KAFKA_IDENTITY_COLUMNS)
    )
    sql = spark.sql.call_args.args[0]
    assert "MERGE INTO local.lake.quarantine_weather_observations AS target" in sql
    for column in KAFKA_IDENTITY_COLUMNS:
        assert f"target.{column} = incoming.{column}" in sql
    assert "WHEN MATCHED" not in sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql
    assert "incoming.validation_error" in sql and "incoming.raw_value" in sql
    spark.sql.return_value.collect.assert_called_once()
    spark.catalog.dropTempView.assert_called_once_with(
        "iceberg_quarantine_weather_batch"
    )
    spark.catalog.dropTempView.reset_mock()
    spark.sql.side_effect = RuntimeError("merge failed")
    with pytest.raises(RuntimeError, match="merge failed"):
        weather_stream.merge_weather_quarantine_batch(batch, 2)
    spark.catalog.dropTempView.assert_called_once()
    spark.sql.reset_mock()
    batch.isEmpty.return_value = True
    weather_stream.merge_weather_quarantine_batch(batch, 3)
    spark.sql.assert_not_called()


def test_kafka_reader_preserves_defaults_and_accepts_weather_topic() -> None:
    spark = MagicMock(spec=SparkSession)
    reader = spark.readStream.format.return_value
    reader.option.return_value = reader
    read_kafka_stream(spark)
    assert (
        call("subscribe", "environment.measurements.canonical")
        in reader.option.call_args_list
    )
    reader.option.reset_mock()
    read_kafka_stream(spark, "environment.weather.canonical", "test:9092")
    assert reader.option.call_args_list == [
        call("kafka.bootstrap.servers", "test:9092"),
        call("subscribe", "environment.weather.canonical"),
        call("startingOffsets", "earliest"),
    ]


def test_weather_query_uses_separate_checkpoint_and_stops_spark_on_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    batch = MagicMock(spec=DataFrame)
    writer = batch.writeStream.foreachBatch.return_value
    writer.outputMode.return_value = writer
    writer.option.return_value = writer
    writer.trigger.return_value = writer
    query = weather_stream.write_weather_quarantine_stream(batch)
    batch.writeStream.foreachBatch.assert_called_once_with(
        weather_stream.merge_weather_quarantine_batch
    )
    writer.outputMode.assert_called_once_with("append")
    writer.option.assert_called_once_with(
        "checkpointLocation", WEATHER_QUARANTINE_CHECKPOINT_PATH
    )
    writer.trigger.assert_called_once_with(availableNow=True)
    assert query is writer.start.return_value
    spark = MagicMock(spec=SparkSession)
    create = Mock(return_value=spark)
    ensure = Mock()
    read = Mock(return_value=batch)
    parse = Mock(return_value=batch)
    invalid = Mock(return_value=batch)
    write = Mock(return_value=query)
    query.awaitTermination.side_effect = RuntimeError("query failed")
    for name, value in {
        "create_spark_session": create,
        "ensure_weather_quarantine_table": ensure,
        "read_kafka_stream": read,
        "parse_kafka_weather": parse,
        "filter_invalid_weather_observations": invalid,
        "write_weather_quarantine_stream": write,
    }.items():
        monkeypatch.setattr(weather_stream, name, value)
    monkeypatch.setattr("sys.argv", ["weather_stream"])
    with pytest.raises(RuntimeError, match="query failed"):
        weather_stream.main()
    create.assert_called_once_with(app_name="weather-quarantine-stream")
    ensure.assert_called_once_with(spark)
    read.assert_called_once_with(
        spark, "environment.weather.canonical", "localhost:9092"
    )
    write.assert_called_once_with(batch, WEATHER_QUARANTINE_CHECKPOINT_PATH)
    spark.stop.assert_called_once()
