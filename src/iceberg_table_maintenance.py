import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from pyspark.sql import Row, SparkSession

if __package__:
    from .measurement_kafka_stream import (
        ICEBERG_CANONICAL_TABLE,
        ICEBERG_CATALOG,
        ICEBERG_HOURLY_AGGREGATE_TABLE,
        ICEBERG_QUARANTINE_TABLE,
        create_spark_session,
    )
else:
    from measurement_kafka_stream import (
        ICEBERG_CANONICAL_TABLE,
        ICEBERG_CATALOG,
        ICEBERG_HOURLY_AGGREGATE_TABLE,
        ICEBERG_QUARANTINE_TABLE,
        create_spark_session,
    )


DEFAULT_TARGET_FILE_SIZE_MB = 128
DEFAULT_MIN_INPUT_FILES = 5
DEFAULT_SNAPSHOT_MAX_AGE_DAYS = 7
DEFAULT_RETAIN_LAST_SNAPSHOTS = 5
BYTES_PER_MEBIBYTE = 1024 * 1024
SMALL_FILE_RATIO = 0.75


@dataclass(frozen=True)
class TableMaintenanceSpec:
    alias: str
    table_name: str

    @property
    def procedure_table_name(self) -> str:
        catalog_prefix = f"{ICEBERG_CATALOG}."
        if not self.table_name.startswith(catalog_prefix):
            raise ValueError(
                f"Table {self.table_name} does not belong to {ICEBERG_CATALOG}."
            )
        return self.table_name.removeprefix(catalog_prefix)


TABLE_MAINTENANCE_SPECS = (
    TableMaintenanceSpec("canonical", ICEBERG_CANONICAL_TABLE),
    TableMaintenanceSpec("quarantine", ICEBERG_QUARANTINE_TABLE),
    TableMaintenanceSpec("hourly", ICEBERG_HOURLY_AGGREGATE_TABLE),
)
TABLE_MAINTENANCE_SPECS_BY_ALIAS = {
    spec.alias: spec for spec in TABLE_MAINTENANCE_SPECS
}


@dataclass(frozen=True)
class IcebergMaintenanceConfig:
    tables: tuple[TableMaintenanceSpec, ...]
    compact: bool = False
    expire_snapshots: bool = False
    target_file_size_mb: int = DEFAULT_TARGET_FILE_SIZE_MB
    min_input_files: int = DEFAULT_MIN_INPUT_FILES
    snapshot_max_age_days: int = DEFAULT_SNAPSHOT_MAX_AGE_DAYS
    retain_last_snapshots: int = DEFAULT_RETAIN_LAST_SNAPSHOTS

    @property
    def target_file_size_bytes(self) -> int:
        return self.target_file_size_mb * BYTES_PER_MEBIBYTE

    @property
    def small_file_threshold_bytes(self) -> int:
        return int(self.target_file_size_bytes * SMALL_FILE_RATIO)


def collect_table_maintenance_state(
    spark: SparkSession,
    spec: TableMaintenanceSpec,
    *,
    small_file_threshold_bytes: int,
    min_input_files: int,
    snapshots_older_than: datetime,
    retain_last_snapshots: int,
) -> dict[str, Any]:
    file_metrics = spark.sql(
        f"""
        SELECT
            COUNT(*) AS data_file_count,
            COALESCE(SUM(record_count), 0) AS data_file_record_count,
            COALESCE(SUM(file_size_in_bytes), 0) AS total_file_size_bytes,
            AVG(file_size_in_bytes) AS average_file_size_bytes,
            MIN(file_size_in_bytes) AS smallest_file_size_bytes,
            MAX(file_size_in_bytes) AS largest_file_size_bytes,
            COALESCE(SUM(
                CASE
                    WHEN file_size_in_bytes < {small_file_threshold_bytes}
                    THEN 1
                    ELSE 0
                END
            ), 0) AS small_file_count
        FROM {spec.table_name}.files
        """
    ).first()
    candidate_metrics = spark.sql(
        f"""
        SELECT
            COUNT(*) AS candidate_partition_count,
            COALESCE(SUM(data_file_count), 0) AS candidate_data_file_count
        FROM (
            SELECT
                partition,
                COUNT(*) AS data_file_count,
                SUM(
                    CASE
                        WHEN file_size_in_bytes < {small_file_threshold_bytes}
                        THEN 1
                        ELSE 0
                    END
                ) AS small_file_count
            FROM {spec.table_name}.files
            GROUP BY partition
            HAVING SUM(
                CASE
                    WHEN file_size_in_bytes < {small_file_threshold_bytes}
                    THEN 1
                    ELSE 0
                END
            ) >= 2
                OR COUNT(*) >= {min_input_files}
        ) candidate_partitions
        """
    ).first()
    snapshot_rows = spark.sql(
        f"""
        SELECT snapshot_id, committed_at, operation
        FROM {spec.table_name}.snapshots
        ORDER BY committed_at DESC
        """
    ).collect()

    snapshots = [_row_to_dict(row) for row in snapshot_rows]
    snapshot_metrics = _build_snapshot_metrics(
        snapshots,
        older_than=snapshots_older_than,
        retain_last_snapshots=retain_last_snapshots,
    )
    file_report = _row_to_dict(file_metrics)
    candidate_report = _row_to_dict(candidate_metrics)

    return {
        "table": spec.table_name,
        "data_files": {
            "count": file_report["data_file_count"],
            "record_count": file_report["data_file_record_count"],
            "total_size_bytes": file_report["total_file_size_bytes"],
            "average_size_bytes": _round_optional(
                file_report["average_file_size_bytes"]
            ),
            "smallest_size_bytes": file_report["smallest_file_size_bytes"],
            "largest_size_bytes": file_report["largest_file_size_bytes"],
            "small_file_count": file_report["small_file_count"],
            "small_file_threshold_bytes": small_file_threshold_bytes,
            "candidate_partition_count": candidate_report[
                "candidate_partition_count"
            ],
            "candidate_file_count": candidate_report[
                "candidate_data_file_count"
            ],
        },
        "snapshots": snapshot_metrics,
        "recommendations": {
            "compact": candidate_report["candidate_partition_count"] > 0,
            "expire_snapshots": (
                snapshot_metrics["age_eligible_snapshot_count"] > 0
            ),
        },
    }


def compact_table(
    spark: SparkSession,
    spec: TableMaintenanceSpec,
    *,
    target_file_size_bytes: int,
    min_input_files: int,
) -> dict[str, Any]:
    result = spark.sql(
        f"""
        CALL {ICEBERG_CATALOG}.system.rewrite_data_files(
            table => '{spec.procedure_table_name}',
            strategy => 'binpack',
            options => map(
                'target-file-size-bytes', '{target_file_size_bytes}',
                'min-input-files', '{min_input_files}'
            )
        )
        """
    ).first()
    return _row_to_dict(result)


def expire_table_snapshots(
    spark: SparkSession,
    spec: TableMaintenanceSpec,
    *,
    older_than: datetime,
    retain_last_snapshots: int,
) -> dict[str, Any]:
    timestamp = _sql_timestamp(older_than)
    result = spark.sql(
        f"""
        CALL {ICEBERG_CATALOG}.system.expire_snapshots(
            table => '{spec.procedure_table_name}',
            older_than => TIMESTAMP '{timestamp}',
            retain_last => {retain_last_snapshots},
            stream_results => true,
            clean_expired_metadata => true
        )
        """
    ).first()
    return _row_to_dict(result)


def run_maintenance(
    spark: SparkSession,
    config: IcebergMaintenanceConfig,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    snapshots_older_than = generated_at - timedelta(
        days=config.snapshot_max_age_days
    )
    table_reports = []

    for spec in config.tables:
        before = collect_table_maintenance_state(
            spark,
            spec,
            small_file_threshold_bytes=config.small_file_threshold_bytes,
            min_input_files=config.min_input_files,
            snapshots_older_than=snapshots_older_than,
            retain_last_snapshots=config.retain_last_snapshots,
        )
        actions = []

        if config.compact:
            actions.append(
                {
                    "action": "compact",
                    "result": compact_table(
                        spark,
                        spec,
                        target_file_size_bytes=config.target_file_size_bytes,
                        min_input_files=config.min_input_files,
                    ),
                }
            )

        if config.expire_snapshots:
            actions.append(
                {
                    "action": "expire_snapshots",
                    "result": expire_table_snapshots(
                        spark,
                        spec,
                        older_than=snapshots_older_than,
                        retain_last_snapshots=config.retain_last_snapshots,
                    ),
                }
            )

        after = (
            collect_table_maintenance_state(
                spark,
                spec,
                small_file_threshold_bytes=config.small_file_threshold_bytes,
                min_input_files=config.min_input_files,
                snapshots_older_than=snapshots_older_than,
                retain_last_snapshots=config.retain_last_snapshots,
            )
            if actions
            else None
        )
        table_reports.append(
            {
                "table": spec.table_name,
                "before": before,
                "actions": actions,
                "after": after,
            }
        )

    return {
        "mode": (
            "execute"
            if config.compact or config.expire_snapshots
            else "report"
        ),
        "generated_at_utc": _serialize_timestamp(generated_at),
        "settings": {
            "target_file_size_bytes": config.target_file_size_bytes,
            "small_file_threshold_bytes": config.small_file_threshold_bytes,
            "min_input_files": config.min_input_files,
            "snapshots_older_than_utc": _serialize_timestamp(
                snapshots_older_than
            ),
            "retain_last_snapshots": config.retain_last_snapshots,
        },
        "tables": table_reports,
    }


def _build_snapshot_metrics(
    snapshots: list[dict[str, Any]],
    *,
    older_than: datetime,
    retain_last_snapshots: int,
) -> dict[str, Any]:
    retained_by_count = snapshots[:retain_last_snapshots]
    expiration_candidates = [
        snapshot
        for snapshot in snapshots[retain_last_snapshots:]
        if _as_utc(snapshot["committed_at"]) < _as_utc(older_than)
    ]
    latest = snapshots[0] if snapshots else None
    oldest = snapshots[-1] if snapshots else None

    return {
        "count": len(snapshots),
        "latest_snapshot_id": latest["snapshot_id"] if latest else None,
        "latest_committed_at_utc": (
            _serialize_timestamp(latest["committed_at"]) if latest else None
        ),
        "latest_operation": latest["operation"] if latest else None,
        "oldest_committed_at_utc": (
            _serialize_timestamp(oldest["committed_at"]) if oldest else None
        ),
        "retained_by_count": len(retained_by_count),
        "age_eligible_snapshot_count": len(expiration_candidates),
    }


def _row_to_dict(row: Row | dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return row
    return row.asDict(recursive=True)


def _round_optional(value: float | None) -> float | None:
    return round(value, 1) if value is not None else None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _serialize_timestamp(value: datetime) -> str:
    return _as_utc(value).isoformat().replace("+00:00", "Z")


def _sql_timestamp(value: datetime) -> str:
    return _as_utc(value).strftime("%Y-%m-%d %H:%M:%S.%f")


def parse_args() -> IcebergMaintenanceConfig:
    parser = argparse.ArgumentParser(
        description=(
            "Report Iceberg maintenance needs and optionally execute explicit "
            "maintenance actions."
        )
    )
    parser.add_argument(
        "--table",
        action="append",
        choices=tuple(TABLE_MAINTENANCE_SPECS_BY_ALIAS),
        dest="table_aliases",
        help="Table alias to inspect. Repeat to select multiple tables.",
    )
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Execute Iceberg bin-pack data-file compaction.",
    )
    parser.add_argument(
        "--expire-snapshots",
        action="store_true",
        help="Expire snapshots older than the configured maximum age.",
    )
    parser.add_argument(
        "--target-file-size-mb",
        type=int,
        default=DEFAULT_TARGET_FILE_SIZE_MB,
        help="Target file size used for compaction and small-file reporting.",
    )
    parser.add_argument(
        "--min-input-files",
        type=int,
        default=DEFAULT_MIN_INPUT_FILES,
        help="Iceberg file-count trigger for compaction within one partition.",
    )
    parser.add_argument(
        "--snapshot-max-age-days",
        type=int,
        default=DEFAULT_SNAPSHOT_MAX_AGE_DAYS,
        help="Expire snapshots older than this number of days when enabled.",
    )
    parser.add_argument(
        "--retain-last-snapshots",
        type=int,
        default=DEFAULT_RETAIN_LAST_SNAPSHOTS,
        help="Minimum number of recent snapshots preserved per table.",
    )
    args = parser.parse_args()

    if args.target_file_size_mb <= 0:
        parser.error("--target-file-size-mb must be greater than zero")
    if args.min_input_files < 2:
        parser.error("--min-input-files must be at least two")
    if args.snapshot_max_age_days <= 0:
        parser.error("--snapshot-max-age-days must be greater than zero")
    if args.retain_last_snapshots <= 0:
        parser.error("--retain-last-snapshots must be greater than zero")

    selected_tables = (
        tuple(
            TABLE_MAINTENANCE_SPECS_BY_ALIAS[alias]
            for alias in dict.fromkeys(args.table_aliases)
        )
        if args.table_aliases
        else TABLE_MAINTENANCE_SPECS
    )
    return IcebergMaintenanceConfig(
        tables=selected_tables,
        compact=args.compact,
        expire_snapshots=args.expire_snapshots,
        target_file_size_mb=args.target_file_size_mb,
        min_input_files=args.min_input_files,
        snapshot_max_age_days=args.snapshot_max_age_days,
        retain_last_snapshots=args.retain_last_snapshots,
    )


def main() -> None:
    config = parse_args()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        report = run_maintenance(spark, config)
    finally:
        spark.stop()

    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
