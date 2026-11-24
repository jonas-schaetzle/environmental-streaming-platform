# Local Linux Bootstrap Verification

The bootstrap package was exercised on 2026-11-23 in a local Ubuntu 24.04
AMD64 container. The unmodified `infra/aws/bootstrap.sh` installed its real Ubuntu
packages, created the shutdown units and added `ubuntu` to the Docker group.
The pinned project dependencies installed successfully in a Python 3.12 venv;
`pip check` reported no broken requirements.

## Reproduce

Start Docker Desktop or an existing local Docker Engine, then run from the
repository root with Bash and Python 3 available:

```bash
bash scripts/verify_linux_bootstrap.sh
```

The runner stages only the bootstrap, probe files, requirements, package metadata
and Python source files. It does not send `.env`, AWS configuration, Terraform
inputs or existing runtime data into the build. The image uses the official
[Ubuntu image](https://hub.docker.com/_/ubuntu/) pinned by digest and explicitly
selects `linux/amd64`. On an ARM workstation this uses local CPU emulation.
Ubuntu package repositories still supply current updates; the recorded versions
describe the tested installation, not a permanent package lock.

The probe downloads OS packages, Python dependencies and the existing pinned
Spark Kafka/Iceberg artifacts. It uses no AWS credentials or cloud services.
Each run creates a fresh ignored `data/verification/linux_bootstrap_*/` directory
with build/probe logs, the source revision, bootstrap hash, image metadata,
installed versions, generated units and an isolated Iceberg warehouse.
The image and exited container are retained for inspection. A failed command
returns non-zero; inspect `build.log` or `probe.log` in the printed directory.

## Verified Versions And Behavior

Evidence from this run is retained under ignored
`data/verification/linux_bootstrap_AgPjTf/`.

| Component | Observed version |
|---|---|
| Ubuntu image architecture | AMD64 / x86_64 |
| Python | 3.12.3 |
| OpenJDK | 21.0.13.1 |
| Docker CLI / installed Engine package | 29.1.3 |
| Docker Compose plugin | 2.40.3 |
| systemd | 255.4-1ubuntu8.19 |
| PySpark | 4.1.1 |
| confluent-kafka | 2.15.1 |
| requests | 2.34.2 |

The bootstrap's three `systemctl` requests were captured in order. Offline
`systemctl enable` created the real timer and Docker unit symlinks;
`systemd-analyze verify` accepted the shutdown service and timer. The probe
confirmed Docker group membership for `ubuntu`.

The existing application Spark factory started successfully on Java 21 and
Python 3.12. It loaded the pinned Kafka source provider and Iceberg 1.11.0,
then wrote and read three rows through the Hadoop Iceberg catalog in UTC.
This is a runtime compatibility check, not a source-ingestion or load test.
The versions are consistent with the supported runtimes in the
[Spark 4.1.1 documentation](https://spark.apache.org/docs/4.1.1/).

## Acceptance Boundary

The container has no systemd PID 1. The probe intercepts service activation and
performs only offline enablement. Docker Engine is installed but is not started
inside the container. No privileged mode or host Docker socket is used.

Cloud-init on the reviewed EC2 AMI, actual Docker service/broker startup, the
active eight-hour timer, EC2 stop behavior and host reboot with retained state
remain host acceptance checks. CPU emulation and this small write/read test do
not establish RAM or storage sufficiency for the real pipeline.

The production bootstrap, event schemas, producer state, Iceberg tables and
streaming checkpoints are unchanged. No data migration or reset is needed.

The [separate sizing mode](runtime-sizing.md) runs the real source-to-Iceberg
jobs and product reports with Kafka alive under one memory limit.
