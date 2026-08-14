from pyspark.sql import SparkSession

from src.openaq_latest_batch import flatten_latest_measurements


def test_flatten_latest_measurements_extracts_expected_columns(
    spark: SparkSession,
) -> None:
    raw = spark.createDataFrame(
        [
            {
                "results": [
                    {
                        "locationsId": 123,
                        "sensorsId": 456,
                        "value": 14.2,
                        "datetime": {
                            "utc": "2026-08-14T10:00:00Z",
                            "local": "2026-08-14T12:00:00+02:00",
                        },
                        "coordinates": {
                            "latitude": 52.52,
                            "longitude": 13.405,
                        },
                    }
                ]
            }
        ]
    )

    result = flatten_latest_measurements(raw)

    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["location_id"] == 123
    assert rows[0]["sensor_id"] == 456
    assert rows[0]["value"] == 14.2
    assert rows[0]["measured_at_utc_raw"] == "2026-08-14T10:00:00Z"
    assert rows[0]["latitude"] == 52.52
    assert rows[0]["longitude"] == 13.405
