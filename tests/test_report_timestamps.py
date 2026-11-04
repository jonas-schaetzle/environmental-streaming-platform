from datetime import datetime, timezone

import pytest
from pyspark.sql import SparkSession

from environmental_streaming.ingestion.openaq_producer import OpenAQLocation
from environmental_streaming.lakehouse.tables import (
    ICEBERG_CANONICAL_TABLE,
    ICEBERG_HOURLY_AGGREGATE_TABLE,
)
from environmental_streaming.products.air_quality_insights import (
    collect_air_quality_report,
)
from environmental_streaming.runtime.timestamps import collect_utc_rows


def test_timestamp_collection_preserves_instants_precision_and_nulls(
    spark: SparkSession, process_timezone: str
) -> None:
    for sql_timezone in ("UTC", "America/New_York"):
        session = spark.newSession()
        session.conf.set("spark.sql.session.timeZone", sql_timezone)
        frame = session.sql(
            """
            SELECT * FROM VALUES
                (1, TIMESTAMP '2026-10-25 00:30:00.123456+00:00'),
                (2, TIMESTAMP '2026-10-25 01:30:00.123456+00:00'),
                (3, TIMESTAMP '1969-12-31 23:59:59.999999+00:00'),
                (4, CAST(NULL AS TIMESTAMP))
            AS observations(id, observed_at)
            ORDER BY id
            """
        )
        rows = collect_utc_rows(frame, ("observed_at",))
        assert rows == [
            {"id": 1, "observed_at": datetime(
                2026, 10, 25, 0, 30, 0, 123456, tzinfo=timezone.utc
            )},
            {"id": 2, "observed_at": datetime(
                2026, 10, 25, 1, 30, 0, 123456, tzinfo=timezone.utc
            )},
            {"id": 3, "observed_at": datetime(
                1969, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc
            )},
            {"id": 4, "observed_at": None},
        ], (process_timezone, sql_timezone)


def test_collected_report_uses_utc_for_freshness_and_hourly_labels(
    spark: SparkSession,
    process_timezone: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = spark.newSession()
    session.conf.set("spark.sql.session.timeZone", "Asia/Tokyo")
    measurements = session.sql(
        """
        SELECT 'openaq' AS source, 2669L AS location_id, parameter,
            parameter AS parameter_display_name, 12.0 AS value, 'ug/m3' AS unit,
            measured_at_utc, measured_at_utc AS kafka_timestamp,
            0 AS kafka_partition, 1L AS kafka_offset
        FROM VALUES
            ('pm25', TIMESTAMP '2026-10-25 00:59:59+00:00'),
            ('no2', TIMESTAMP '2026-10-25 01:00:00+00:00')
        AS observations(parameter, measured_at_utc)
        """
    )
    aggregates = session.sql(
        """
        SELECT 'openaq' AS source, 2669L AS location_id, 'pm25' AS parameter,
            'ug/m3' AS unit,
            TIMESTAMP '2026-10-25 00:00:00+00:00' AS window_start,
            TIMESTAMP '2026-10-25 01:00:00+00:00' AS window_end,
            TIMESTAMP '2026-10-25 00:30:00+00:00' AS latest_measured_at_utc,
            4L AS measurement_count, 12.0 AS average_value,
            10.0 AS minimum_value, 14.0 AS maximum_value
        """
    )
    tables = {
        ICEBERG_CANONICAL_TABLE: measurements,
        ICEBERG_HOURLY_AGGREGATE_TABLE: aggregates,
    }
    monkeypatch.setattr(session, "table", tables.__getitem__)
    report = collect_air_quality_report(
        session,
        (OpenAQLocation(location_id=2669, name="Munich Stachus"),),
        now=datetime(2026, 10, 25, 3, tzinfo=timezone.utc),
        freshness_threshold_seconds=7200,
    )
    location = report["locations"][0]
    by_parameter = {item["parameter"]: item for item in location["parameters"]}
    assert location["latest_measured_at_utc"] == "2026-10-25T01:00:00Z"
    assert location["freshness_seconds"] == 7200
    assert by_parameter["pm25"]["status"] == "stale", process_timezone
    assert by_parameter["pm25"]["freshness_seconds"] == 7201
    assert by_parameter["no2"]["status"] == "fresh"
    hour = by_parameter["pm25"]["latest_finalized_hour"]
    assert hour["window_start_utc"] == "2026-10-25T00:00:00Z"
    assert hour["window_end_utc"] == "2026-10-25T01:00:00Z"
    assert hour["latest_measured_at_utc"] == "2026-10-25T00:30:00Z"
