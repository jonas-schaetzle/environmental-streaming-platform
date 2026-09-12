from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count, window
from pyspark.sql.streaming import StreamingQuery

if __package__:
    from .measurement_file_stream import (
        STREAM_INPUT_PATH,
        canonical_measurement_schema,
        create_spark_session,
        filter_valid_measurements,
    )
else:
    from measurement_file_stream import (
        STREAM_INPUT_PATH,
        canonical_measurement_schema,
        create_spark_session,
        filter_valid_measurements,
    )


WINDOW_OUTPUT_PATH = "data/stream/output/measurement_windows"
WINDOW_CHECKPOINT_PATH = "data/stream/checkpoints/measurement_windows"
WATERMARK_DELAY = "0 seconds"
WINDOW_DURATION = "5 minutes"


def read_measurement_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream
        .schema(canonical_measurement_schema())
        .json(STREAM_INPUT_PATH)
    )


def aggregate_measurements_by_window(measurements: DataFrame) -> DataFrame:
    return (
        filter_valid_measurements(measurements)
        .withWatermark("measured_at_utc", WATERMARK_DELAY)
        .groupBy(
            window("measured_at_utc", WINDOW_DURATION),
            "source",
            "parameter",
            "unit",
        )
        .agg(
            count("*").alias("measurement_count"),
            avg("value").alias("avg_value"),
        )
        .select(
            col("window.start").alias("window_start_utc"),
            col("window.end").alias("window_end_utc"),
            "source",
            "parameter",
            "unit",
            "measurement_count",
            "avg_value",
        )
    )


def write_window_stream(windowed_measurements: DataFrame) -> StreamingQuery:
    return (
        windowed_measurements.writeStream
        .format("parquet")
        .outputMode("append")
        .option("checkpointLocation", WINDOW_CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start(WINDOW_OUTPUT_PATH)
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    measurements = read_measurement_stream(spark)
    windowed_measurements = aggregate_measurements_by_window(measurements)

    query = write_window_stream(windowed_measurements)
    query.awaitTermination()

    spark.stop()


if __name__ == "__main__":
    main()
