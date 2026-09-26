import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pyspark.sql import SparkSession

if __package__:
    from .measurement_kafka_stream import (
        HOURLY_AGGREGATE_IDENTITY_COLUMNS,
        ICEBERG_CANONICAL_TABLE,
        ICEBERG_HOURLY_AGGREGATE_TABLE,
        ICEBERG_QUARANTINE_TABLE,
        KAFKA_IDENTITY_COLUMNS,
        create_spark_session,
    )
else:
    from measurement_kafka_stream import (
        HOURLY_AGGREGATE_IDENTITY_COLUMNS,
        ICEBERG_CANONICAL_TABLE,
        ICEBERG_HOURLY_AGGREGATE_TABLE,
        ICEBERG_QUARANTINE_TABLE,
        KAFKA_IDENTITY_COLUMNS,
        create_spark_session,
    )


@dataclass(frozen=True)
class TableAuditSpec:
    table_name: str
    identity_columns: tuple[str, ...]
    time_column: str
    expected_partition: str


TABLE_AUDIT_SPECS = (
    TableAuditSpec(
        table_name=ICEBERG_CANONICAL_TABLE,
        identity_columns=KAFKA_IDENTITY_COLUMNS,
        time_column="measured_at_utc",
        expected_partition="days(measured_at_utc)",
    ),
    TableAuditSpec(
        table_name=ICEBERG_QUARANTINE_TABLE,
        identity_columns=KAFKA_IDENTITY_COLUMNS,
        time_column="kafka_timestamp",
        expected_partition="days(kafka_timestamp)",
    ),
    TableAuditSpec(
        table_name=ICEBERG_HOURLY_AGGREGATE_TABLE,
        identity_columns=HOURLY_AGGREGATE_IDENTITY_COLUMNS,
        time_column="latest_measured_at_utc",
        expected_partition="days(window_start)",
    ),
)


def collect_table_audit(
    spark: SparkSession,
    spec: TableAuditSpec,
) -> dict[str, Any]:
    null_identity_condition = " OR ".join(
        f"{column} IS NULL" for column in spec.identity_columns
    )
    identity_columns = ", ".join(spec.identity_columns)

    table_metrics = spark.sql(
        f"""
        SELECT
            COUNT(*) AS row_count,
            COALESCE(
                SUM(CASE WHEN {null_identity_condition} THEN 1 ELSE 0 END),
                0
            ) AS null_identity_rows,
            MAX({spec.time_column}) AS latest_timestamp
        FROM {spec.table_name}
        """
    ).first()
    duplicate_metrics = spark.sql(
        f"""
        SELECT COALESCE(SUM(identity_count - 1), 0) AS duplicate_rows
        FROM (
            SELECT COUNT(*) AS identity_count
            FROM {spec.table_name}
            GROUP BY {identity_columns}
            HAVING COUNT(*) > 1
        )
        """
    ).first()
    file_metrics = spark.sql(
        f"""
        SELECT
            COUNT(*) AS data_file_count,
            COALESCE(SUM(record_count), 0) AS data_file_record_count
        FROM {spec.table_name}.files
        """
    ).first()
    latest_snapshot = spark.sql(
        f"""
        SELECT
            COUNT(*) AS snapshot_count,
            max_by(snapshot_id, committed_at) AS snapshot_id,
            MAX(committed_at) AS committed_at,
            max_by(operation, committed_at) AS operation
        FROM {spec.table_name}.snapshots
        """
    ).first()
    partition_row = (
        spark.sql(f"DESCRIBE TABLE EXTENDED {spec.table_name}")
        .where("col_name = 'Part 0'")
        .first()
    )

    actual_partition = partition_row["data_type"] if partition_row else None
    duplicate_rows = duplicate_metrics["duplicate_rows"]
    null_identity_rows = table_metrics["null_identity_rows"]
    healthy = (
        duplicate_rows == 0
        and null_identity_rows == 0
        and actual_partition == spec.expected_partition
    )

    return {
        "table": spec.table_name,
        "status": "healthy" if healthy else "unhealthy",
        "row_count": table_metrics["row_count"],
        "duplicate_rows": duplicate_rows,
        "null_identity_rows": null_identity_rows,
        "identity_columns": list(spec.identity_columns),
        "latest_timestamp": _serialize_timestamp(table_metrics["latest_timestamp"]),
        "partition": actual_partition,
        "expected_partition": spec.expected_partition,
        "data_file_count": file_metrics["data_file_count"],
        "data_file_record_count": file_metrics["data_file_record_count"],
        "snapshot_count": latest_snapshot["snapshot_count"] if latest_snapshot else 0,
        "latest_snapshot_id": (
            latest_snapshot["snapshot_id"] if latest_snapshot else None
        ),
        "latest_snapshot_at": (
            _serialize_timestamp(latest_snapshot["committed_at"])
            if latest_snapshot
            else None
        ),
        "latest_snapshot_operation": (
            latest_snapshot["operation"] if latest_snapshot else None
        ),
    }


def collect_iceberg_audit(spark: SparkSession) -> dict[str, Any]:
    tables = [collect_table_audit(spark, spec) for spec in TABLE_AUDIT_SPECS]

    return {
        "status": (
            "healthy"
            if all(table["status"] == "healthy" for table in tables)
            else "unhealthy"
        ),
        "tables": tables,
    }


def _serialize_timestamp(value: object) -> str | None:
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.isoformat()

    return str(value)


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        report = collect_iceberg_audit(spark)
    finally:
        spark.stop()

    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))

    if report["status"] != "healthy":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
