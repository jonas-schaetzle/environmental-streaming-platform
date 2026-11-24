# Local Runtime Sizing

## Reproduce The Bounded Workload

Start local Docker and make the existing `OPENAQ_API_KEY` available in the shell
environment privately, as for normal ingestion. Do not put the key in a command
argument or Terraform input. From the repository root, run:

```bash
bash scripts/verify_linux_bootstrap.sh --sizing
```

This extends the [Ubuntu bootstrap probe](linux-bootstrap-verification.md) with
the pinned Kafka 4.2.1 distribution. Kafka and all child processes run inside
one AMD64 Linux cgroup limited to two CPUs and 6 GiB, with swap disabled. No
ports are published. Only the OpenAQ producer receives the API key; it is passed
at runtime and never copied into the image or build context. The retained image
metadata contains no key. Do not export the container's runtime environment.

The run formats a fresh isolated KRaft directory, starts Kafka with a 1 GiB
heap and creates both canonical topics with three partitions. It polls all three
configured locations once per source, with a six-hour weather overlap. It then
executes the existing measurement job, both weather sinks, the all-source audit,
the 48-hour joint report, latest-value report and quality alerts. Spark processes
run sequentially while Kafka remains alive. The measurement job retains its
existing three concurrent streaming queries within one Spark process.

The runner uses a fresh ignored `data/verification/runtime_sizing_*/` directory
as the container's project `data/`. Source state, checkpoints and lake files from
other runs are never mounted. It stops the broker at the end, retains the exited
container/image and evidence, and returns non-zero for a failed phase or an OOM
event. See `probe.log`, per-phase stdout/stderr and `phase-results.json` when
investigating a failure. Ubuntu package versions are recorded by the separate
bootstrap mode; package repositories remain updateable.

The first local run exposed a Python-worker startup failure in the joint report:
invoking `.venv/bin/python` did not add a `python3` command to the child process's
PATH. The application factory now defaults `PYSPARK_PYTHON` to the running
interpreter and preserves explicit overrides, following
[Spark's Python runtime setting](https://spark.apache.org/docs/4.1.1/configuration.html#environment-variables).
A regression starts real Python workers with an unusable `python3` first on PATH.
This changes no event contracts, checkpoint formats or stored rows.

## Measurement Method

The probe samples cgroup `memory.current`, anonymous memory and file cache every
200 ms, retaining the phase name alongside each sample. `memory.peak` provides
the kernel-recorded lifetime high-water mark, including charged file cache and
all processes in the cgroup. `memory.events` verifies that no OOM or OOM kill
occurred. These measurements follow the
[Linux cgroup v2 interface](https://docs.kernel.org/admin-guide/cgroup-v2.html).
They exclude the Docker daemon and the host OS; shared page-cache accounting can
also differ from a fresh standalone host.

Per-phase disk snapshots retain both allocated and apparent bytes for runtime
data, Kafka, Iceberg, checkpoints, the Python environment and Maven dependencies.
The Docker image's uncompressed layer size is recorded in `image.json`. Runtime
data contains some nested components, and the image already contains the venv;
do not sum overlapping figures. Sparse Kafka index files can make apparent size
larger than allocated space. A single cycle cannot establish long-term growth.

## Verified Result On 2026-11-24

The completed run is retained under ignored
`data/verification/runtime_sizing_vZoK5L/`. All ten phases exited successfully.
The producers published 20 OpenAQ and 126 weather events; canonical counts
matched both totals, both quarantine tables were empty, and the audit was
healthy. The hourly table held five finalized aggregates. The joint, latest and
alert reports all completed with the interpreter fix.

| Measurement | Observed result |
|---|---:|
| Combined cgroup memory peak | 2.09 GiB (2,245,996,544 bytes) |
| Memory limit / swap limit | 6 GiB / 0 |
| OOM, OOM-kill and memory-limit events | 0 |
| Runtime data, allocated / apparent | 41.52 / 140.77 MiB |
| Kafka within runtime data, allocated / apparent | 40.18 / 140.10 MiB |
| Iceberg within runtime data, allocated | 0.29 MiB |
| Checkpoints within runtime data, allocated | 0.31 MiB |
| Python venv within image, allocated | 570.64 MiB |
| Additional Maven cache, allocated | 216.24 MiB |
| Uncompressed image layers, including OS/tools/venv/Kafka | 2.33 GiB |

The kernel peak also matched the highest lifetime peak in the retained samples.
The source-to-canonical count reconciliation and component summaries are in
`evidence/summary.json`; full phase/disk results are in `evidence/report.json`.
The broker was alive throughout all seven Spark processes and stopped cleanly
afterward. The previous bootstrap mode also passed after the runner extension.

This is one small live collection cycle. Old OpenAQ sensor timestamps and the
short weather range leave report gaps; the run does not demonstrate dense
history or complete city comparisons. It supports retaining the current 8 GiB /
40 GiB candidate for bounded integration, with substantial observed headroom,
subject to the host checks below. It does not establish maximum throughput or
multi-day storage growth. No AWS resources were created or resized.

## EC2 Decision Boundary

The existing candidate is [t3.large](https://aws.amazon.com/ec2/instance-types/t3/)
with two vCPUs and 8 GiB, Standard CPU credits, and one encrypted 40 GiB gp3 root
volume. The 6 GiB container limit reserves 2 GiB of that candidate's capacity for
the OS and Docker in the local sizing model. This is a planning reserve, not a
measurement of the deployed OS. Keep the existing candidate pending its live
integration check; the local probe does not authorize provisioning.

The ARM workstation emulates AMD64. Durations are retained to diagnose this run,
not to forecast EC2 throughput or session length. Standard CPU-credit throttling,
cloud-init, Docker service overhead, larger Kafka backlogs, report history and
long-lived state must be checked on the host. Keep Spark processes sequential.
Before a bounded session, check real memory availability and free disk with the
broker running, and retain at least 10 GiB free disk as an operating margin.
If that margin is unavailable, export/review retained evidence and re-evaluate
storage; do not delete Kafka data, checkpoints or lake files automatically.
