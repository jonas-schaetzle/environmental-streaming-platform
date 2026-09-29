import os
import platform
import shutil
import subprocess
from pathlib import Path

from pyspark.sql import SparkSession

from environmental_streaming.lakehouse.tables import ICEBERG_CATALOG


SUPPORTED_JAVA_VERSIONS = ("21", "17")
HOMEBREW_JDK_HOMES = (
    Path("/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home"),
    Path("/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"),
    Path("/opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home"),
    Path("/usr/local/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home"),
    Path("/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"),
    Path("/usr/local/opt/openjdk/libexec/openjdk.jdk/Contents/Home"),
)
SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
ICEBERG_SPARK_PACKAGE = (
    "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
)
SPARK_PACKAGES = ",".join((SPARK_KAFKA_PACKAGE, ICEBERG_SPARK_PACKAGE))
LOCAL_SHUFFLE_PARTITIONS = "4"
ICEBERG_WAREHOUSE_PATH = str(
    Path(__file__).resolve().parents[3] / "data" / "warehouse"
)


def create_spark_session() -> SparkSession:
    configure_java_runtime()

    return (
        SparkSession.builder.appName("measurement-kafka-stream")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", LOCAL_SHUFFLE_PARTITIONS)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse",
            ICEBERG_WAREHOUSE_PATH,
        )
        .getOrCreate()
    )


def configure_java_runtime() -> str | None:
    java_home = os.environ.get("JAVA_HOME")

    if java_home and _java_binary_exists(Path(java_home)):
        return java_home

    if _java_on_path_works():
        return java_home

    discovered_java_home = discover_java_home()

    if discovered_java_home:
        os.environ["JAVA_HOME"] = str(discovered_java_home)
        os.environ["PATH"] = (
            f"{discovered_java_home / 'bin'}{os.pathsep}{os.environ.get('PATH', '')}"
        )
        return str(discovered_java_home)

    raise RuntimeError(
        "No supported Java runtime found. Spark 4.1 requires Java 17 or 21. "
        "Install one with `brew install openjdk@21` on macOS, or set JAVA_HOME "
        "to an existing JDK installation."
    )


def discover_java_home() -> Path | None:
    for version in SUPPORTED_JAVA_VERSIONS:
        java_home = _macos_java_home(version)

        if java_home and _java_binary_exists(java_home):
            return java_home

    for java_home in HOMEBREW_JDK_HOMES:
        if _java_binary_exists(java_home):
            return java_home

    return None


def _macos_java_home(version: str) -> Path | None:
    if platform.system() != "Darwin":
        return None

    result = subprocess.run(
        ["/usr/libexec/java_home", "-v", version],
        capture_output=True,
        check=False,
        text=True,
    )

    if result.returncode != 0:
        return None

    java_home = result.stdout.strip()

    return Path(java_home) if java_home else None


def _java_binary_exists(java_home: Path) -> bool:
    return (java_home / "bin" / "java").exists()


def _java_on_path_works() -> bool:
    if not shutil.which("java"):
        return False

    try:
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            check=False,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False

    return result.returncode == 0
