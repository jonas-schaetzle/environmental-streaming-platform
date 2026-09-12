from pyspark.sql import SparkSession
from pyspark.sql.types import BinaryType, LongType, StringType, StructField, StructType

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
