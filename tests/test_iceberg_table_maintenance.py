from datetime import datetime, timezone
from unittest.mock import MagicMock

from pyspark.sql import SparkSession

from environmental_streaming.lakehouse.maintenance import (
    IcebergMaintenanceConfig,
    TableMaintenanceSpec,
    collect_table_maintenance_state,
    compact_table,
    expire_table_snapshots,
    run_maintenance,
)


def _query_result(
    *,
    first: dict[str, object] | None = None,
    collect: list[dict[str, object]] | None = None,
) -> MagicMock:
    result = MagicMock()
    result.first.return_value = first
    result.collect.return_value = collect or []
    return result


def test_collect_state_reports_partition_candidates_and_snapshot_retention() -> None:
    spark = MagicMock(spec=SparkSession)
    spark.sql.side_effect = [
        _query_result(
            first={
                "data_file_count": 8,
                "data_file_record_count": 26,
                "total_file_size_bytes": 48_000,
                "average_file_size_bytes": 6_000.25,
                "smallest_file_size_bytes": 5_800,
                "largest_file_size_bytes": 6_400,
                "small_file_count": 8,
            }
        ),
        _query_result(
            first={
                "candidate_partition_count": 1,
                "candidate_data_file_count": 5,
            }
        ),
        _query_result(
            collect=[
                {
                    "snapshot_id": 3,
                    "committed_at": datetime(2026, 9, 28, 10),
                    "operation": "append",
                },
                {
                    "snapshot_id": 2,
                    "committed_at": datetime(2026, 9, 20, 10),
                    "operation": "append",
                },
                {
                    "snapshot_id": 1,
                    "committed_at": datetime(2026, 9, 10, 10),
                    "operation": "append",
                },
            ]
        ),
    ]
    spec = TableMaintenanceSpec(
        alias="canonical",
        table_name="local.lake.canonical_measurements",
    )

    report = collect_table_maintenance_state(
        spark,
        spec,
        small_file_threshold_bytes=100_000,
        min_input_files=5,
        snapshots_older_than=datetime(2026, 9, 21, tzinfo=timezone.utc),
        retain_last_snapshots=1,
    )

    assert report["data_files"] == {
        "count": 8,
        "record_count": 26,
        "total_size_bytes": 48_000,
        "average_size_bytes": 6_000.2,
        "smallest_size_bytes": 5_800,
        "largest_size_bytes": 6_400,
        "small_file_count": 8,
        "small_file_threshold_bytes": 100_000,
        "candidate_partition_count": 1,
        "candidate_file_count": 5,
    }
    assert report["snapshots"] == {
        "count": 3,
        "latest_snapshot_id": 3,
        "latest_committed_at_utc": "2026-09-28T10:00:00Z",
        "latest_operation": "append",
        "oldest_committed_at_utc": "2026-09-10T10:00:00Z",
        "retained_by_count": 1,
        "age_eligible_snapshot_count": 2,
    }
    assert report["recommendations"] == {
        "compact": True,
        "expire_snapshots": True,
    }
    candidate_sql = spark.sql.call_args_list[1].args[0]
    assert "GROUP BY partition" in candidate_sql
    assert ") >= 2" in candidate_sql
    assert "COUNT(*) >= 5" in candidate_sql


def test_maintenance_procedures_use_allowlisted_table_and_safe_options() -> None:
    spark = MagicMock(spec=SparkSession)
    spark.sql.side_effect = [
        _query_result(
            first={
                "rewritten_data_files_count": 5,
                "added_data_files_count": 1,
                "rewritten_bytes_count": 48_000,
                "failed_data_files_count": 0,
                "removed_delete_files_count": 0,
            }
        ),
        _query_result(
            first={
                "deleted_data_files_count": 4,
                "deleted_manifest_files_count": 2,
            }
        ),
    ]
    spec = TableMaintenanceSpec(
        alias="canonical",
        table_name="local.lake.canonical_measurements",
    )

    compact_result = compact_table(
        spark,
        spec,
        target_file_size_bytes=134_217_728,
        min_input_files=5,
    )
    expiration_result = expire_table_snapshots(
        spark,
        spec,
        older_than=datetime(2026, 9, 21, 12, tzinfo=timezone.utc),
        retain_last_snapshots=5,
    )

    compact_sql = spark.sql.call_args_list[0].args[0]
    assert "CALL local.system.rewrite_data_files" in compact_sql
    assert "table => 'lake.canonical_measurements'" in compact_sql
    assert "'target-file-size-bytes', '134217728'" in compact_sql
    assert "'min-input-files', '5'" in compact_sql
    assert compact_result["rewritten_data_files_count"] == 5

    expiration_sql = spark.sql.call_args_list[1].args[0]
    assert "CALL local.system.expire_snapshots" in expiration_sql
    assert "TIMESTAMP '2026-09-21 12:00:00.000000'" in expiration_sql
    assert "retain_last => 5" in expiration_sql
    assert "stream_results => true" in expiration_sql
    assert expiration_result["deleted_data_files_count"] == 4


def test_run_maintenance_is_read_only_without_action_flags(
    monkeypatch,
) -> None:
    spark = MagicMock(spec=SparkSession)
    spec = TableMaintenanceSpec(
        alias="canonical",
        table_name="local.lake.canonical_measurements",
    )
    state = {
        "table": spec.table_name,
        "data_files": {},
        "snapshots": {},
        "recommendations": {},
    }
    collect_state = MagicMock(return_value=state)
    compact = MagicMock()
    expire = MagicMock()
    monkeypatch.setattr(
        "environmental_streaming.lakehouse.maintenance."
        "collect_table_maintenance_state",
        collect_state,
    )
    monkeypatch.setattr(
        "environmental_streaming.lakehouse.maintenance.compact_table",
        compact,
    )
    monkeypatch.setattr(
        "environmental_streaming.lakehouse.maintenance."
        "expire_table_snapshots",
        expire,
    )
    config = IcebergMaintenanceConfig(tables=(spec,))

    report = run_maintenance(
        spark,
        config,
        now=datetime(2026, 9, 28, 12, tzinfo=timezone.utc),
    )

    assert report["mode"] == "report"
    assert report["tables"][0] == {
        "table": spec.table_name,
        "before": state,
        "actions": [],
        "after": None,
    }
    collect_state.assert_called_once()
    compact.assert_not_called()
    expire.assert_not_called()
