"""Record a local Ubuntu package probe; do not claim service/EC2 acceptance."""

import hashlib
import json
import platform
import shutil
import subprocess
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path

from environmental_streaming.runtime.spark import (
    ICEBERG_SPARK_PACKAGE,
    SPARK_KAFKA_PACKAGE,
    create_spark_session,
)


def output(*command: str) -> str:
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    return (result.stdout + result.stderr).strip()


def main() -> None:
    evidence = Path("/evidence")
    requests = Path("/opt/probe/systemctl-requests.log").read_text().splitlines()
    assert requests == [
        "daemon-reload",
        "enable --now environmental-demo-stop.timer",
        "enable --now docker",
    ]
    assert "docker" in output("id", "-nG", "ubuntu").split()
    for unit, target in (
        ("environmental-demo-stop.timer", "timers.target"),
        ("docker.service", "multi-user.target"),
    ):
        assert Path(f"/etc/systemd/system/{target}.wants/{unit}").is_symlink()
    assert platform.machine() == "x86_64"
    assert output("dpkg", "--print-architecture") == "amd64"

    versions = {
        "python": platform.python_version(),
        "java": output("java", "-version"),
        "docker": output("docker", "--version"),
        "compose": output("docker", "compose", "version"),
        "systemd": output("systemd-analyze", "--version"),
        "python_packages": {
            package: version(package)
            for package in ("pyspark", "confluent-kafka", "requests", "pytest", "ruff")
        },
        "os_packages": output(
            "dpkg-query",
            "-W",
            "-f=${Package}=${Version}\n",
            "ca-certificates",
            "docker.io",
            "docker-compose-v2",
            "openjdk-21-jre-headless",
            "python3.12-venv",
            "systemd",
        ).splitlines(),
    }
    for name in ("environmental-demo-stop.service", "environmental-demo-stop.timer"):
        shutil.copyfile(Path("/etc/systemd/system") / name, evidence / name)
    shutil.copyfile(
        "/opt/probe/systemctl-requests.log", evidence / "systemctl-requests.log"
    )
    report = {
        "verified_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "local Ubuntu amd64 container; service activation intercepted",
        "architecture": platform.machine(),
        "bootstrap_sha256": hashlib.sha256(
            Path("/opt/probe/bootstrap.sh").read_bytes()
        ).hexdigest(),
        "versions": versions,
        "systemd_unit_syntax": "verified during image build",
        "service_activation": "not exercised",
        "pending_host_checks": [
            "cloud-init on the reviewed AMI",
            "Docker daemon and broker startup",
            "timer activation and EC2 stop",
            "host reboot with retained data",
            "memory and storage sizing",
        ],
    }
    spark = create_spark_session("linux-bootstrap-probe")
    try:
        spark.conf.set("spark.sql.catalog.local.warehouse", str(evidence / "warehouse"))
        spark.sparkContext.setLogLevel("WARN")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.bootstrap_probe")
        spark.range(3).writeTo("local.bootstrap_probe.runtime_check").using(
            "iceberg"
        ).create()
        assert [
            row.id
            for row in spark.table("local.bootstrap_probe.runtime_check")
            .orderBy("id")
            .collect()
        ] == [0, 1, 2]
        assert spark._jvm.java.lang.Class.forName(
            "org.apache.spark.sql.kafka010.KafkaSourceProvider",
            True,
            spark._jvm.java.lang.Thread.currentThread().getContextClassLoader(),
        )
        report["spark_smoke"] = {
            "spark_version": spark.version,
            "session_timezone": spark.conf.get("spark.sql.session.timeZone"),
            "iceberg_rows_written_and_read": 3,
            "iceberg_package": ICEBERG_SPARK_PACKAGE,
            "kafka_package": SPARK_KAFKA_PACKAGE,
            "kafka_provider_loaded": True,
        }
    finally:
        spark.stop()
    (evidence / "report.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
