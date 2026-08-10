from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, max as spark_max, min as spark_min
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType


INPUT_PATH = "data/input/air_quality_sample.csv"
OUTPUT_PATH = "data/output/air_quality_by_city_measurement"


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


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("air-quality-batch")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    readings = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(INPUT_PATH)
    )

    readings.printSchema()
    readings.show(truncate=False)

    aggregates = (
        readings
        .groupBy("city", "measurement_type", "unit")
        .agg(
            count("*").alias("reading_count"),
            avg("value").alias("avg_value"),
            spark_min("value").alias("min_value"),
            spark_max("value").alias("max_value"),
        )
        .orderBy(col("city"), col("measurement_type"))
    )

    aggregates.show(truncate=False)

    (
        aggregates
        .write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )

    spark.stop()


if __name__ == "__main__":
    main()
