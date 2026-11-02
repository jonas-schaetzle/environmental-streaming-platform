from datetime import datetime
from unittest.mock import MagicMock, Mock

import pytest
from pyspark.sql import SparkSession

from environmental_streaming.lakehouse import audit as iceberg_table_audit
from environmental_streaming.lakehouse.audit import (
    TableAuditSpec,
    collect_table_audit,
)
from environmental_streaming.lakehouse.tables import (
    ICEBERG_WEATHER_CANONICAL_TABLE,
    ICEBERG_WEATHER_QUARANTINE_TABLE,
    KAFKA_IDENTITY_COLUMNS,
)
from environmental_streaming.processing.weather_model import WEATHER_IDENTITY_COLUMNS


def _query_result(row: dict[str, object] | None) -> MagicMock:
    result = MagicMock()
    result.first.return_value = row
    return result


def _audit_spark(
    *,
    duplicate_rows: int = 0,
    null_identity_rows: int = 0,
    partition: str = "days(measured_at_utc)",
    latest_snapshot: dict[str, object] | None = None,
) -> MagicMock:
    spark = MagicMock(spec=SparkSession)
    description = MagicMock()
    description.where.return_value.first.return_value = {"data_type": partition}
    spark.sql.side_effect = [
        _query_result(
            {
                "row_count": 26,
                "null_identity_rows": null_identity_rows,
                "latest_timestamp": datetime(2026, 9, 21, 8, 0),
            }
        ),
        _query_result({"duplicate_rows": duplicate_rows}),
        _query_result({"data_file_count": 7, "data_file_record_count": 26}),
        _query_result(latest_snapshot),
        description,
    ]
    return spark


def test_collect_table_audit_reports_healthy_table() -> None:
    latest_snapshot = {
        "snapshot_id": 42,
        "committed_at": datetime(2026, 9, 24, 15, 39),
        "operation": "append",
        "snapshot_count": 2,
    }
    spark = _audit_spark(latest_snapshot=latest_snapshot)
    spec = TableAuditSpec(
        table_name="local.lake.canonical_measurements",
        identity_columns=("kafka_topic", "kafka_partition", "kafka_offset"),
        time_column="measured_at_utc",
        expected_partition="days(measured_at_utc)",
    )

    report = collect_table_audit(spark, spec)

    assert report == {
        "table": "local.lake.canonical_measurements",
        "status": "healthy",
        "row_count": 26,
        "duplicate_rows": 0,
        "null_identity_rows": 0,
        "identity_columns": [
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
        ],
        "latest_timestamp": "2026-09-21T08:00:00",
        "partition": "days(measured_at_utc)",
        "expected_partition": "days(measured_at_utc)",
        "data_file_count": 7,
        "data_file_record_count": 26,
        "snapshot_count": 2,
        "latest_snapshot_id": 42,
        "latest_snapshot_at": "2026-09-24T15:39:00",
        "latest_snapshot_operation": "append",
    }
    sql_calls = [call.args[0] for call in spark.sql.call_args_list]
    assert "GROUP BY kafka_topic, kafka_partition, kafka_offset" in sql_calls[1]
    assert "local.lake.canonical_measurements.files" in sql_calls[2]
    assert "local.lake.canonical_measurements.snapshots" in sql_calls[3]


def test_collect_table_audit_marks_invalid_identity_or_partition_unhealthy() -> None:
    spark = _audit_spark(
        duplicate_rows=2,
        null_identity_rows=1,
        partition="days(kafka_timestamp)",
        latest_snapshot=None,
    )
    spec = TableAuditSpec(
        table_name="local.lake.canonical_measurements",
        identity_columns=("kafka_topic", "kafka_partition", "kafka_offset"),
        time_column="measured_at_utc",
        expected_partition="days(measured_at_utc)",
    )

    report = collect_table_audit(spark, spec)

    assert report["status"] == "unhealthy"
    assert report["duplicate_rows"] == 2
    assert report["null_identity_rows"] == 1
    assert report["snapshot_count"] == 0
    assert report["latest_snapshot_id"] is None


def test_weather_audits_use_business_and_transport_identities() -> None:
    canonical, quarantine = iceberg_table_audit.WEATHER_TABLE_AUDIT_SPECS
    assert canonical.table_name == ICEBERG_WEATHER_CANONICAL_TABLE
    assert canonical.identity_columns == WEATHER_IDENTITY_COLUMNS
    assert canonical.time_column == "measured_at_utc"
    assert quarantine.table_name == ICEBERG_WEATHER_QUARANTINE_TABLE
    assert quarantine.identity_columns == KAFKA_IDENTITY_COLUMNS
    assert quarantine.time_column == "kafka_timestamp"
    for spec in (canonical, quarantine):
        for duplicates, nulls, partition in (
            (0, 0, spec.expected_partition),
            (1, 0, spec.expected_partition),
            (0, 1, spec.expected_partition),
            (0, 0, "unexpected_partition"),
        ):
            spark = _audit_spark(
                duplicate_rows=duplicates,
                null_identity_rows=nulls,
                partition=partition,
            )
            report = collect_table_audit(spark, spec)
            healthy = duplicates == nulls == 0 and partition == spec.expected_partition
            assert report["status"] == ("healthy" if healthy else "unhealthy")
            assert report["duplicate_rows"] == duplicates
            assert report["null_identity_rows"] == nulls
            assert report["identity_columns"] == list(spec.identity_columns)
            queries = [call.args[0] for call in spark.sql.call_args_list]
            assert f"MAX({spec.time_column})" in queries[0]
            assert f"GROUP BY {', '.join(spec.identity_columns)}" in queries[1]
            assert all(
                query.lstrip().startswith(("SELECT", "DESCRIBE")) for query in queries
            )


def test_collect_iceberg_audit_preserves_default_and_aggregates_selected_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spark = MagicMock(spec=SparkSession)
    collect = Mock(return_value={"status": "healthy"})
    monkeypatch.setattr(iceberg_table_audit, "collect_table_audit", collect)
    assert iceberg_table_audit.collect_iceberg_audit(spark)["status"] == "healthy"
    assert [call.args[1] for call in collect.call_args_list] == list(
        iceberg_table_audit.TABLE_AUDIT_SPECS
    )
    for source in ("weather", "all"):
        collect.reset_mock()
        specs = iceberg_table_audit.TABLE_AUDIT_SPECS_BY_SOURCE[source]
        collect.side_effect = [
            {"status": "healthy"} for _ in specs[:-1]
        ] + [{"status": "unhealthy"}]
        report = iceberg_table_audit.collect_iceberg_audit(spark, specs)
        assert report["status"] == "unhealthy"
        assert len(report["tables"]) == len(specs)
        assert [call.args[1] for call in collect.call_args_list] == list(specs)


@pytest.mark.parametrize("source", [None, "openaq", "weather", "all"])
@pytest.mark.parametrize("status", ["healthy", "unhealthy"])
def test_main_selects_tables_and_exits_with_report_status(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    source: str | None,
    status: str,
) -> None:
    spark = MagicMock(spec=SparkSession)
    monkeypatch.setattr(
        "sys.argv", ["audit", *(["--source", source] if source else [])]
    )
    monkeypatch.setattr(iceberg_table_audit, "create_spark_session", lambda: spark)
    collect = Mock(return_value={"status": status, "tables": []})
    monkeypatch.setattr(iceberg_table_audit, "collect_iceberg_audit", collect)

    if status == "unhealthy":
        with pytest.raises(SystemExit) as error:
            iceberg_table_audit.main()
        assert error.value.code == 1
    else:
        iceberg_table_audit.main()

    collect.assert_called_once_with(
        spark, iceberg_table_audit.TABLE_AUDIT_SPECS_BY_SOURCE[source or "openaq"]
    )
    assert f'"status": "{status}"' in capsys.readouterr().out
    spark.stop.assert_called_once_with()


def test_main_does_not_hide_missing_tables_and_stops_spark(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spark = MagicMock(spec=SparkSession)
    monkeypatch.setattr("sys.argv", ["audit", "--source", "weather"])
    monkeypatch.setattr(iceberg_table_audit, "create_spark_session", lambda: spark)
    monkeypatch.setattr(
        iceberg_table_audit,
        "collect_iceberg_audit",
        Mock(side_effect=RuntimeError("table missing")),
    )
    with pytest.raises(RuntimeError, match="table missing"):
        iceberg_table_audit.main()
    spark.stop.assert_called_once_with()
