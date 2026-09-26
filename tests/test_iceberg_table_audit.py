from datetime import datetime
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

from src import iceberg_table_audit
from src.iceberg_table_audit import TableAuditSpec, collect_table_audit


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


def test_main_exits_with_error_for_unhealthy_report(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    spark = MagicMock(spec=SparkSession)
    monkeypatch.setattr(iceberg_table_audit, "create_spark_session", lambda: spark)
    monkeypatch.setattr(
        iceberg_table_audit,
        "collect_iceberg_audit",
        lambda _spark: {"status": "unhealthy", "tables": []},
    )

    with pytest.raises(SystemExit) as error:
        iceberg_table_audit.main()

    assert error.value.code == 1
    assert '"status": "unhealthy"' in capsys.readouterr().out
    spark.stop.assert_called_once_with()
