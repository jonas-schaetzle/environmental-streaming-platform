from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, explode, to_timestamp


INPUT_PATH = "data/input/openaq_location_latest_raw.json"
OUTPUT_PATH = "data/output/openaq_location_latest"
SENSOR_METADATA_INPUT_PATH = "data/input/openaq_sensor_raw.json"


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


def read_openaq_sensor_metadata_raw(spark: SparkSession) -> DataFrame:
    return (
        spark.read
        .option("multiLine", True)
        .json(SENSOR_METADATA_INPUT_PATH)
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


def flatten_sensor_metadata(raw: DataFrame) -> DataFrame:
    return (
        raw
        .select(explode("results").alias("result"))
        .select(
            col("result.id").alias("sensor_id"),
            col("result.parameter.name").alias("parameter"),
            col("result.parameter.units").alias("unit"),
            col("result.parameter.displayName").alias("parameter_display_name"),
        )
    )


def write_latest_measurements(measurements: DataFrame) -> None:
    (
        measurements
        .write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )


def enrich_latest_measurements(
    latest_measurements: DataFrame,
    sensor_metadata: DataFrame,
) -> DataFrame:
    return (
        latest_measurements
        .join(sensor_metadata, on="sensor_id", how="left")
        .select(
            "location_id",
            "sensor_id",
            "parameter",
            "parameter_display_name",
            "value",
            "unit",
            "measured_at_utc_raw",
            "measured_at_utc",
            "latitude",
            "longitude",
        )
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    latest_raw = read_openaq_latest_raw(spark)
    sensor_metadata_raw = read_openaq_sensor_metadata_raw(spark)

    latest_measurements = flatten_latest_measurements(latest_raw)
    sensor_metadata = flatten_sensor_metadata(sensor_metadata_raw)
    measurements = enrich_latest_measurements(latest_measurements, sensor_metadata)

    measurements.printSchema()
    measurements.show(truncate=False)

    write_latest_measurements(measurements)

    spark.stop()


if __name__ == "__main__":
    main()
