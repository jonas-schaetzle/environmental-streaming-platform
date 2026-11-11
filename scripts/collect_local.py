"""Run bounded local collection and drain the existing Kafka topics into Iceberg."""

import argparse
import fcntl
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from tempfile import mkdtemp

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient

from environmental_streaming.messaging.kafka_publisher import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    WEATHER_KAFKA_TOPIC,
)


ROOT = Path(__file__).resolve().parents[1]
COLLECTION_DIR = ROOT / "data" / "collection"
STEPS = (
    ("measurements", "processing.measurement_stream"),
    ("weather-canonical", "processing.weather_stream", "--sink", "canonical"),
    ("weather-quarantine", "processing.weather_stream", "--sink", "quarantine"),
)


def positive_int(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return number


def run_step(directory: Path, name: str, module: str, *args: str, lock_fd: int) -> int:
    print(f"Starting {name}", flush=True)
    with (
        (directory / f"{name}.stdout").open("w", encoding="utf-8") as stdout,
        (directory / f"{name}.stderr").open("w", encoding="utf-8") as stderr,
    ):
        result = subprocess.run(
            [sys.executable, "-m", f"environmental_streaming.{module}", *args],
            cwd=ROOT,
            stdout=stdout,
            stderr=stderr,
            check=False,
            pass_fds=(lock_fd,),
        )
    print(f"Finished {name} with exit code {result.returncode}", flush=True)
    return result.returncode


def collect(args: argparse.Namespace, lock_fd: int) -> int:
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    topics = admin.list_topics(timeout=15).topics
    missing = {KAFKA_TOPIC, WEATHER_KAFKA_TOPIC} - topics.keys()
    if missing:
        raise RuntimeError(
            f"Create the required Kafka topics first: {', '.join(sorted(missing))}"
        )

    started = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    directory = Path(mkdtemp(prefix=f"{started}_", dir=COLLECTION_DIR))
    print(f"Collection logs: {directory.relative_to(ROOT)}", flush=True)
    statuses = []
    for cycle in range(1, args.cycles + 1):
        statuses.append(
            run_step(
                directory,
                f"openaq-{cycle:03}",
                "ingestion.openaq_producer",
                lock_fd=lock_fd,
            )
        )
        statuses.append(
            run_step(
                directory,
                f"weather-{cycle:03}",
                "ingestion.weather_producer",
                "--all-locations",
                "--past-hours",
                str(args.weather_past_hours),
                "--state-path",
                "data/state/weather_kafka_producer.json",
                lock_fd=lock_fd,
            )
        )
        if cycle < args.cycles:
            time.sleep(args.interval_seconds)

    for name, module, *options in STEPS:
        statuses.append(run_step(directory, name, module, *options, lock_fd=lock_fd))

    if any(statuses):
        print("One or more steps failed. See the collection logs.", file=sys.stderr)
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cycles", type=positive_int, default=1)
    parser.add_argument("--interval-seconds", type=positive_int, default=900)
    parser.add_argument("--weather-past-hours", type=positive_int, default=6)
    args = parser.parse_args()
    COLLECTION_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with (COLLECTION_DIR / ".lock").open("a", encoding="utf-8") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return collect(args, lock.fileno())
    except BlockingIOError:
        print("A local collection run is already active.", file=sys.stderr)
    except (KafkaException, RuntimeError) as error:
        print(f"Local collection failed: {error}", file=sys.stderr)
    except KeyboardInterrupt:
        return 130
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
