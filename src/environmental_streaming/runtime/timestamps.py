from datetime import datetime, timedelta, timezone
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import unix_micros


UTC_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def utc_datetime_from_micros(value: int | None) -> datetime | None:
    """Decode Spark epoch microseconds without the Python host timezone."""
    return UTC_EPOCH + timedelta(microseconds=value) if value is not None else None


def collect_utc_rows(
    frame: DataFrame, timestamp_columns: tuple[str, ...]
) -> list[dict[str, Any]]:
    """Collect selected TimestampType fields as timezone-aware UTC datetimes."""
    rows = frame.withColumns(
        {name: unix_micros(name) for name in timestamp_columns}
    ).collect()
    result = []
    for row in rows:
        values = row.asDict(recursive=True)
        for name in timestamp_columns:
            values[name] = utc_datetime_from_micros(values[name])
        result.append(values)
    return result
