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
    CANONICAL_WEATHER_COLUMNS,
    KAFKA_IDENTITY_COLUMNS,
    QUARANTINE_WEATHER_COLUMNS,
    WEATHER_CANONICAL_CHECKPOINT_PATH,
    WEATHER_QUARANTINE_CHECKPOINT_PATH,
    ensure_weather_canonical_table,
    ensure_weather_quarantine_table,
)
from environmental_streaming.processing import weather_stream
from environmental_streaming.processing.measurement_stream import read_kafka_stream
from environmental_streaming.processing.weather_model import (
    canonical_weather_schema,
    filter_invalid_weather_observations,
    filter_valid_weather_observations,
    parse_weather_observations,
)


def _valid_weather_batch(spark: SparkSession, revisions: list[dict]) -> DataFrame:
    base = {
        "source": "open_meteo",
        "source_dataset": "forecast_best_match",
        "weather_location_id": "munich",
        "city": "Munich",
        "latitude": 48.13743,
        "longitude": 11.57549,
        "grid_latitude": 48.14,
        "grid_longitude": 11.58,
        "parameter": "temperature_2m",
        "value": 10.0,
        "unit": "degC",
        "measured_at_utc": "2026-10-17T10:00:00Z",
        "fetched_at_utc": "2026-10-17T10:15:00Z",
    }
    raw = spark.createDataFrame(
        [
            (
                "environment.weather.canonical",
                0,
                offset,
                datetime(2026, 10, 17, 11, tzinfo=timezone.utc),
                "open_meteo:munich",
                json.dumps({**base, **revision}),
            )
            for offset, revision in enumerate(revisions)
        ],
        [
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "kafka_key",
            "raw_value",
        ],
    )
    return filter_valid_weather_observations(parse_weather_observations(raw))


def test_canonical_weather_table_enforces_payload_contract_and_hour_partitions() -> (
    None
):
    spark = MagicMock(spec=SparkSession)
    ensure_weather_canonical_table(spark)
    assert spark.sql.call_count == 2
    ddl = spark.sql.call_args.args[0]
    assert "CREATE TABLE IF NOT EXISTS local.lake.canonical_weather_observations" in ddl
    assert "PARTITIONED BY (days(measured_at_utc))" in ddl
    assert "'format-version' = '2'" in ddl
    assert CANONICAL_WEATHER_COLUMNS == QUARANTINE_WEATHER_COLUMNS[:-1]
    for field in canonical_weather_schema():
        assert f"{field.name} {field.dataType.simpleString().upper()} NOT NULL" in ddl


def test_canonical_reducer_chooses_newest_revision_and_rejects_equal_receipt_conflicts(
    spark: SparkSession,
) -> None:
    batch = _valid_weather_batch(
        spark,
        [
            {"value": 9.0, "fetched_at_utc": "2026-10-17T10:05:00Z"},
            {"value": 12.0},
            {"value": 12.0},
            {"value": 8.0, "fetched_at_utc": "2026-10-17T10:01:00Z"},
            {"parameter": "relative_humidity_2m", "unit": "%", "value": 55.0},
        ],
    )
    rows = weather_stream.prepare_weather_canonical_batch(batch).collect()
    assert len(rows) == 2
    temperature = next(row for row in rows if row.parameter == "temperature_2m")
    assert temperature.value == 12.0 and temperature.kafka_offset == 1
    for change in (
        {"value": 11.0},
        {"city": "Another label"},
        {"grid_latitude": 48.15},
    ):
        with pytest.raises(ValueError, match="Conflicting weather content"):
            weather_stream.prepare_weather_canonical_batch(
                _valid_weather_batch(spark, [{}, change])
            )
    equivalent = _valid_weather_batch(
        spark, [{}, {"fetched_at_utc": "2026-10-17T10:15:00+00:00"}]
    )
    assert weather_stream.prepare_weather_canonical_batch(equivalent).count() == 1


def test_canonical_merge_guards_stored_revisions_and_cleans_up(
    spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = _valid_weather_batch(spark, [{}])
    sql = Mock()
    monkeypatch.setattr(spark, "table", Mock(return_value=target))
    monkeypatch.setattr(spark, "sql", sql)
    monkeypatch.setattr(spark.catalog, "refreshTable", Mock())
    with pytest.raises(ValueError, match="currently stored revision"):
        weather_stream.merge_weather_canonical_batch(
            _valid_weather_batch(spark, [{"value": 11.0}]), 1
        )
    sql.assert_not_called()
    old = _valid_weather_batch(
        spark, [{"value": 9.0, "fetched_at_utc": "2026-10-17T10:05:00Z"}]
    )
    weather_stream.merge_weather_canonical_batch(old, 2)
    statement = sql.call_args.args[0]
    assert "MERGE INTO local.lake.canonical_weather_observations AS target" in statement
    assert (
        "WHEN MATCHED AND incoming.fetched_at_utc > target.fetched_at_utc" in statement
    )
    assert (
        "target.kafka_topic = incoming.kafka_topic" in statement
    )  # Accepted lineage is updated.
    assert "ON target.source = incoming.source" in statement
    sql.return_value.collect.assert_called_once()
    assert not spark.catalog.tableExists("iceberg_canonical_weather_batch")
    sql.side_effect = RuntimeError("merge failed")
    with pytest.raises(RuntimeError, match="merge failed"):
        weather_stream.merge_weather_canonical_batch(old, 3)
    assert not spark.catalog.tableExists("iceberg_canonical_weather_batch")
    empty = MagicMock(spec=DataFrame)
    cached = empty.select.return_value.persist.return_value
    cached.isEmpty.return_value = True
    empty.sparkSession = MagicMock(spec=SparkSession)
    weather_stream.merge_weather_canonical_batch(empty, 4)
    cached.unpersist.assert_called_once()
    empty.sparkSession.sql.assert_not_called()


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


@pytest.mark.parametrize("sink", ["quarantine", "canonical"])
def test_weather_query_uses_separate_checkpoint_and_stops_spark_on_failure(
    monkeypatch: pytest.MonkeyPatch,
    sink: str,
) -> None:
    batch = MagicMock(spec=DataFrame)
    writer = batch.writeStream.foreachBatch.return_value
    writer.outputMode.return_value = writer
    writer.option.return_value = writer
    writer.trigger.return_value = writer
    merge = (
        weather_stream.merge_weather_canonical_batch
        if sink == "canonical"
        else weather_stream.merge_weather_quarantine_batch
    )
    checkpoint = (
        WEATHER_CANONICAL_CHECKPOINT_PATH
        if sink == "canonical"
        else WEATHER_QUARANTINE_CHECKPOINT_PATH
    )
    write_stream = (
        weather_stream.write_weather_canonical_stream
        if sink == "canonical"
        else weather_stream.write_weather_quarantine_stream
    )
    query = write_stream(batch)
    batch.writeStream.foreachBatch.assert_called_once_with(merge)
    writer.outputMode.assert_called_once_with("append")
    writer.option.assert_called_once_with("checkpointLocation", checkpoint)
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
        f"ensure_weather_{sink}_table": ensure,
        "read_kafka_stream": read,
        "parse_kafka_weather": parse,
        "filter_valid_weather_observations"
        if sink == "canonical"
        else "filter_invalid_weather_observations": invalid,
        f"write_weather_{sink}_stream": write,
    }.items():
        monkeypatch.setattr(weather_stream, name, value)
    monkeypatch.setattr(
        "sys.argv",
        ["weather_stream", *(["--sink", "canonical"] if sink == "canonical" else [])],
    )
    with pytest.raises(RuntimeError, match="query failed"):
        weather_stream.main()
    create.assert_called_once_with(app_name=f"weather-{sink}-stream")
    ensure.assert_called_once_with(spark)
    read.assert_called_once_with(
        spark, "environment.weather.canonical", "localhost:9092"
    )
    write.assert_called_once_with(batch, checkpoint)
    spark.stop.assert_called_once()
