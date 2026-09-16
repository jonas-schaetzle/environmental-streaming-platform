from pyspark.sql import SparkSession
from pyspark.sql.types import BinaryType, LongType, StringType, StructField, StructType

from src.measurement_file_stream import filter_invalid_measurements
from src.measurement_kafka_stream import parse_kafka_measurements


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
                "environment.measurements.raw",
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

    assert row["kafka_topic"] == "environment.measurements.raw"
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
                "environment.measurements.raw",
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

    assert row["kafka_topic"] == "environment.measurements.raw"
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
                "environment.measurements.raw",
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
