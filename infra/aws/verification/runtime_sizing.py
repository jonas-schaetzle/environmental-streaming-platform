"""Measure a bounded live pipeline with Kafka and Spark in one Linux cgroup."""

import json
import os
import platform
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka.admin import AdminClient, NewTopic

from environmental_streaming.messaging.kafka_publisher import (
    KAFKA_TOPIC,
    WEATHER_KAFKA_TOPIC,
)


DATA = Path("/opt/project/data")
CGROUP = Path("/sys/fs/cgroup")
KAFKA = Path("/opt/kafka/bin")
MODULE = "environmental_streaming."
GIB = 1024**3


def counters(name: str) -> dict[str, int]:
    return {
        key: int(value)
        for key, value in (
            line.split() for line in (CGROUP / name).read_text().splitlines()
        )
    }


def memory() -> dict:
    stat = counters("memory.stat")
    return {
        "current_bytes": int((CGROUP / "memory.current").read_text()),
        "lifetime_peak_bytes": int((CGROUP / "memory.peak").read_text()),
        "anonymous_bytes": stat["anon"],
        "file_cache_bytes": stat["file"],
    }


def disk() -> dict:
    result = {}
    for name, path in (
        ("runtime", DATA),
        ("kafka", DATA / "kafka"),
        ("warehouse", DATA / "warehouse"),
        ("checkpoints", DATA / "checkpoints"),
        ("python_venv", Path("/opt/project/.venv")),
        ("maven_cache", Path("/root/.ivy2.5.2")),
    ):
        if path.exists():
            sizes = []
            for extra in ([], ["--apparent-size"]):
                output = subprocess.check_output(
                    ["du", "-sx", "--block-size=1", *extra, str(path)], text=True
                )
                sizes.append(int(output.split()[0]))
            result[name] = dict(zip(("allocated_bytes", "logical_bytes"), sizes))
    return result


def main() -> None:
    api_key = os.environ.pop("OPENAQ_API_KEY")
    assert api_key, "An existing OpenAQ key is required; do not print it."
    assert platform.machine() == "x86_64"
    assert int((CGROUP / "memory.max").read_text()) == 6 * GIB
    assert (CGROUP / "memory.swap.max").read_text().strip() == "0"
    assert not (DATA / "kafka").exists(), "Use a new isolated run directory."
    started = datetime.now(timezone.utc).isoformat()
    before_events = counters("memory.events")
    stop_sampling = threading.Event()
    phase = "setup"
    phases = []

    def sample() -> None:
        with (DATA / "memory-samples.jsonl").open("w") as output:
            while not stop_sampling.is_set():
                output.write(
                    json.dumps(
                        {
                            "monotonic_seconds": time.monotonic(),
                            "phase": phase,
                            **memory(),
                        }
                    )
                    + "\n"
                )
                output.flush()
                stop_sampling.wait(0.2)

    sampler = threading.Thread(target=sample, daemon=True)
    sampler.start()

    def step(name: str, command: list[str], extra_env: dict | None = None) -> None:
        nonlocal phase
        phase = name
        print(f"Starting {name}", flush=True)
        begin = time.monotonic()
        with (
            (DATA / f"{name}.stdout").open("w") as stdout,
            (DATA / f"{name}.stderr").open("w") as stderr,
        ):
            result = subprocess.run(
                command,
                stdout=stdout,
                stderr=stderr,
                timeout=900,
                env={**os.environ, **(extra_env or {})},
                check=False,
            )
        phases.append(
            {
                "name": name,
                "exit_code": result.returncode,
                "elapsed_seconds": round(time.monotonic() - begin, 2),
                "memory_at_end": memory(),
                "disk_at_end": disk(),
            }
        )
        if result.returncode:
            raise RuntimeError(f"{name} failed; inspect its retained stderr.")

    def module(name: str, module_name: str, *args: str, extra_env=None) -> None:
        step(name, [sys.executable, "-m", MODULE + module_name, *args], extra_env)

    properties = DATA / "broker.properties"
    properties.write_text(
        "\n".join(
            [
                "process.roles=broker,controller",
                "node.id=1",
                "controller.quorum.voters=1@127.0.0.1:9093",
                "listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093",
                "advertised.listeners=PLAINTEXT://127.0.0.1:9092",
                "controller.listener.names=CONTROLLER",
                "listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
                f"log.dirs={DATA / 'kafka'}",
                "num.partitions=3",
                "offsets.topic.replication.factor=1",
                "transaction.state.log.replication.factor=1",
                "transaction.state.log.min.isr=1",
                "group.initial.rebalance.delay.ms=0",
                "auto.create.topics.enable=false",
                "\n",
            ]
        )
    )
    broker = None
    try:
        step(
            "format-kafka",
            [
                str(KAFKA / "kafka-storage.sh"),
                "format",
                "-t",
                uuid.uuid4().hex,
                "-c",
                str(properties),
            ],
        )
        with (DATA / "broker.log").open("w") as log:
            broker = subprocess.Popen(
                [str(KAFKA / "kafka-server-start.sh"), str(properties)],
                stdout=log,
                stderr=subprocess.STDOUT,
                env={**os.environ, "KAFKA_HEAP_OPTS": "-Xms1G -Xmx1G"},
            )
        admin = AdminClient({"bootstrap.servers": "127.0.0.1:9092"})
        for _ in range(60):
            if broker.poll() is not None:
                raise RuntimeError("Kafka exited before readiness; inspect broker.log.")
            try:
                admin.list_topics(timeout=1)
                break
            except Exception:
                time.sleep(1)
        else:
            raise TimeoutError("Kafka did not become ready.")
        for future in admin.create_topics(
            [
                NewTopic(KAFKA_TOPIC, 3, 1),
                NewTopic(WEATHER_KAFKA_TOPIC, 3, 1),
            ]
        ).values():
            future.result(timeout=30)
        phase = "broker-idle"
        time.sleep(2)
        idle_memory = memory()
        module(
            "openaq", "ingestion.openaq_producer", extra_env={"OPENAQ_API_KEY": api_key}
        )
        module(
            "weather",
            "ingestion.weather_producer",
            "--all-locations",
            "--past-hours",
            "6",
            "--state-path",
            "data/state/weather.json",
        )
        for name, path, args in (
            ("measurements", "processing.measurement_stream", ()),
            ("weather-canonical", "processing.weather_stream", ("--sink", "canonical")),
            (
                "weather-quarantine",
                "processing.weather_stream",
                ("--sink", "quarantine"),
            ),
            ("audit", "lakehouse.audit", ("--source", "all")),
            ("hourly", "products.hourly_environmental_insights", ("--hours", "48")),
            ("latest", "products.air_quality_insights", ()),
            ("alerts", "products.data_quality_alerts", ()),
        ):
            module(name, path, *args)
        audit = json.loads((DATA / "audit.stdout").read_text())
        assert audit["status"] == "healthy"
        assert (
            json.loads((DATA / "weather.stdout").read_text())["locations_failed"] == 0
        )
        assert broker.poll() is None, "Kafka must remain alive for the entire run."
        after_events = counters("memory.events")
        assert after_events["oom"] == before_events["oom"]
        assert after_events["oom_kill"] == before_events["oom_kill"]
        report = {
            "started_at_utc": started,
            "finished_at_utc": datetime.now(timezone.utc).isoformat(),
            "scope": "one live multi-location cycle; sequential Spark processes with Kafka alive",
            "architecture": platform.machine(),
            "cpu_max": (CGROUP / "cpu.max").read_text().strip(),
            "memory_limit_bytes": 6 * GIB,
            "swap_limit_bytes": 0,
            "broker_heap": "-Xms1G -Xmx1G",
            "broker_idle_memory": idle_memory,
            "combined_memory": memory(),
            "memory_events_before": before_events,
            "memory_events_after": after_events,
            "tables": [
                {"table": row["table"], "row_count": row["row_count"]}
                for row in audit["tables"]
            ],
            "phases": phases,
            "disk": disk(),
        }
        (DATA / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    finally:
        if broker is not None and broker.poll() is None:
            broker.terminate()
            try:
                broker.wait(timeout=45)
            except subprocess.TimeoutExpired:
                broker.kill()
                broker.wait()
        stop_sampling.set()
        sampler.join(timeout=5)
        (DATA / "phase-results.json").write_text(json.dumps(phases, indent=2) + "\n")


if __name__ == "__main__":
    main()
