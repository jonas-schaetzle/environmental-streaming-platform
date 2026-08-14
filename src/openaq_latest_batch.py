from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, explode, to_timestamp


INPUT_PATH = "data/input/openaq_location_latest_raw.json"
OUTPUT_PATH = "data/output/openaq_location_latest"


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("openaq-latest-batch")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_openaq_latest_raw(spark: SparkSession) -> DataFrame:
    return (
        spark.read
        .option("multiline", True)
        .json(INPUT_PATH)
    )


def flatten_latest_measurements(raw: DataFrame) -> DataFrame:
    return (
        raw
        .select(explode("results").alias("result"))
        .select(
            col("result.locationsId").alias("location_id"),
            col("result.sensorsId").alias("sensor_id"),
            col("result.value").alias("value"),
            col("result.datetime.utc").alias("measured_at_utc_raw"),
            to_timestamp("result.datetime.utc").alias("measured_at_utc"),
            col("result.coordinates.latitude").alias("latitude"),
            col("result.coordinates.longitude").alias("longitude"),
        )
    )


def write_latest_measurements(measurements: DataFrame) -> None:
    (
        measurements
        .write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw = read_openaq_latest_raw(spark)
    measurements = flatten_latest_measurements(raw)

    measurements.printSchema()
    measurements.show(truncate=False)

    write_latest_measurements(measurements)

    spark.stop()


if __name__ == "__main__":
    main()
