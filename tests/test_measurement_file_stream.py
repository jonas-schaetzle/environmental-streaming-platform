from pyspark.sql.types import DoubleType, LongType, StringType, TimestampType

from src.measurement_file_stream import canonical_measurement_schema


def test_canonical_measurement_schema_matches_documented_field_order() -> None:
    schema = canonical_measurement_schema()

    assert schema.fieldNames() == [
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
    ]

    assert isinstance(schema["source"].dataType, StringType)
    assert isinstance(schema["location_id"].dataType, LongType)
    assert isinstance(schema["sensor_id"].dataType, LongType)
    assert isinstance(schema["value"].dataType, DoubleType)
    assert isinstance(schema["measured_at_utc"].dataType, TimestampType)
