import argparse
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import struct, to_json

if __package__:
    from .spark_runtime import configure_java_runtime
else:
    from spark_runtime import configure_java_runtime


DEFAULT_INPUT_PATH = Path("data/output/openaq_location_latest")
DEFAULT_OUTPUT_PATH = Path("data/stream/input/openaq_location_latest.jsonl")


@dataclass(frozen=True)
class ExportConfig:
    input_path: Path = DEFAULT_INPUT_PATH
    output_path: Path = DEFAULT_OUTPUT_PATH


def create_spark_session() -> SparkSession:
    configure_java_runtime()

    return (
        SparkSession.builder
        .appName("canonical-jsonl-export")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_canonical_measurements(spark: SparkSession, input_path: Path) -> DataFrame:
    return spark.read.parquet(str(input_path))


def measurements_to_json_lines(measurements: DataFrame) -> list[str]:
    return [
        row["json"]
        for row in measurements.select(to_json(struct("*")).alias("json")).collect()
    ]


def write_jsonl(json_lines: list[str], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(json_lines) + "\n", encoding="utf-8")


def export_canonical_measurements(
    spark: SparkSession,
    input_path: Path,
    output_path: Path,
) -> int:
    measurements = read_canonical_measurements(spark, input_path)
    json_lines = measurements_to_json_lines(measurements)
    write_jsonl(json_lines, output_path)

    return len(json_lines)


def parse_args() -> ExportConfig:
    parser = argparse.ArgumentParser(
        description="Export canonical measurement Parquet data to a local JSONL file."
    )
    parser.add_argument(
        "--input-path",
        type=Path,
        default=DEFAULT_INPUT_PATH,
        help="Path to canonical measurement Parquet data.",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Path to write JSONL events.",
    )

    args = parser.parse_args()

    return ExportConfig(input_path=args.input_path, output_path=args.output_path)


def main() -> None:
    config = parse_args()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    exported_count = export_canonical_measurements(
        spark,
        input_path=config.input_path,
        output_path=config.output_path,
    )
    print(f"Exported {exported_count} events to {config.output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
