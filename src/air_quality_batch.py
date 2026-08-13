from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count, max as spark_max, min as spark_min
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType


INPUT_PATH = "data/input/air_quality_sample.csv"
OUTPUT_PATH = "data/output/air_quality_by_city_measurement"
LOCAL_SHUFFLE_PARTITIONS = "4"


schema = StructType(
    [
        StructField("station_id", StringType(), nullable=False),
        StructField("city", StringType(), nullable=False),
        StructField("measurement_type", StringType(), nullable=False),
        StructField("value", DoubleType(), nullable=False),
        StructField("unit", StringType(), nullable=False),
        StructField("measured_at", TimestampType(), nullable=False),
    ]
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("air-quality-batch")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", LOCAL_SHUFFLE_PARTITIONS)
        .getOrCreate()
    )


def read_air_quality_readings(spark: SparkSession) -> DataFrame:
    return (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(INPUT_PATH)
    )


def filter_valid_readings(readings: DataFrame) -> DataFrame:
    return readings.filter(col("value").isNotNull() & (col("value") >= 0))


def aggregate_air_quality(readings: DataFrame) -> DataFrame:
    return (
        readings
        .groupBy("city", "measurement_type", "unit")
        .agg(
            count("*").alias("reading_count"),
            avg("value").alias("avg_value"),
            spark_min("value").alias("min_value"),
            spark_max("value").alias("max_value")
        )
        .orderBy(col("city"), col("measurement_type"))
    )


def write_aggregates(aggregates: DataFrame) -> None:
    (
        aggregates
        .write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    readings = read_air_quality_readings(spark)

    readings.printSchema()
    readings.show(truncate=False)

    valid_readings = filter_valid_readings(readings)
    aggregates = aggregate_air_quality(valid_readings)

    aggregates.show(truncate=False)
    write_aggregates(aggregates)

    spark.stop()


if __name__ == "__main__":
    main()
