from collections.abc import Generator

import pytest
from pyspark.sql import SparkSession

from src.spark_runtime import configure_java_runtime


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession]:
    configure_java_runtime()

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
