from collections.abc import Generator
import os
import time

import pytest
from pyspark.sql import SparkSession

from environmental_streaming.runtime.spark import configure_java_runtime


@pytest.fixture(params=("UTC", "Europe/Berlin"))
def process_timezone(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[str]:
    if not hasattr(time, "tzset"):
        pytest.skip("Changing the process timezone requires time.tzset")
    previous = os.environ.get("TZ")
    monkeypatch.setenv("TZ", request.param)
    time.tzset()
    try:
        yield request.param
    finally:
        if previous is None:
            monkeypatch.delenv("TZ", raising=False)
        else:
            monkeypatch.setenv("TZ", previous)
        time.tzset()


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession]:
    configure_java_runtime()

    session = (
        SparkSession.builder.appName("environmental-streaming-platform-tests")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")

    yield session

    session.stop()
