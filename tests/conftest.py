from collections.abc import Generator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession]:
    session = (
        SparkSession.builder
        .appName("environmental-streaming-platform-tests")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")

    yield session

    session.stop()
