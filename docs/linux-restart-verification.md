# Linux Process Restart Verification

## Run With Retained Real Data

Start local Docker and select a completed, stopped
[runtime-sizing run](runtime-sizing.md). Its `evidence/` directory must still
contain the complete Kafka directory, warehouse, checkpoints, producer state and
broker properties. Do not copy a running broker. From the repository root:

```bash
bash scripts/verify_linux_bootstrap.sh --restart \
  data/verification/runtime_sizing_vZoK5L/evidence
```

The source must be under `data/verification/`. The runner rejects its named
container if it is running, checks the successful sizing phase results, and
copies the complete recovery components into a new ignored
`data/verification/linux_restart_*/evidence/` directory. It rejects symlinks and
compares source/copy file hashes. The original source is never mounted in the
test container; its captured file hashes are checked again after success.
The old container need not exist when a complete stopped archive is retained.
Keep every writer to that archive stopped for the entire copy and check.

The probe uses the pinned Ubuntu AMD64 runtime and Kafka distribution, with
two CPUs, a 6 GiB memory limit and no swap. It needs no API credentials or AWS
resources. It uses the real source events and state retained by the sizing run.
No Kafka formatting, topic creation, state reset or history deletion occurs.
The fixed `/opt/project/data` mount preserves paths embedded in Iceberg metadata
and Spark checkpoints.

## What The Probe Checks

1. Start Kafka from the copied state. Read both topics with automatic commits
   and automatic offset storage disabled. Capture cluster/topic identity, each
   partition's earliest/end offsets and hashes of retained keys, payloads,
   timestamps and offsets. Read all five Iceberg tables and checkpoint metadata.
2. Stop Kafka cleanly and start a new broker process against the same directory.
   Compare the Kafka inventory. In a fresh Python/JVM process, start the three
   measurement sinks and both weather sinks with their existing checkpoints.
   Require zero input, identical table rows and snapshot IDs, the original query
   IDs and the original committed source offsets.
3. Publish one saved canonical weather event unchanged, preserving its receipt
   timestamp. Capture the pending Kafka inventory, then stop/start Kafka again
   before Spark processes that record. Require identical retained Kafka inventory
   after restart. Run the five queries in another fresh Python/JVM process.
4. Require one input record in each weather query, zero in the measurement
   queries, original query IDs, new run IDs and committed offsets at the Kafka
   end. Canonical values and accepted Kafka lineage must remain identical for
   the replayed weather revision. Preserve all original checkpoint files and
   producer-state hashes. Stop the test broker cleanly afterward.

The JSON reports retain Spark application/query/run IDs, committed offsets,
table row hashes/snapshots and both Kafka inventories. Per-phase stdout/stderr
and broker logs remain in the new evidence directory. `archive-manifest.json`
records the hashes of the original recovery files.

## Verified Result On 2026-11-25

Evidence is retained under ignored `data/verification/linux_restart_bjNzVv/`.
All three Spark inspection/processing phases and both broker restarts completed.
The test container exited successfully and the broker stopped cleanly.

| Check | Result |
|---|---|
| Original recovery files | All 189 captured file hashes unchanged |
| Producer state | Both files unchanged |
| Existing checkpoints | All 80 original files preserved byte-for-byte |
| No-input query runs | Zero input in all five queries; original query IDs and offsets |
| No-input Iceberg results | All five row hashes, counts and snapshot IDs unchanged |
| Kafka restarts | Same cluster/topic identity, partition ranges and retained-record hashes |
| Pending weather replay | Partition 0, offset 126 survived restart; both weather queries consumed it |
| Continued progress | Both weather checkpoints advanced from end offset 126 to 127 |
| Canonical rows | 20 air-quality, 126 weather, five finalized hourly aggregates |
| Quarantine rows | Zero for both sources |

The resumed runs retained their query IDs and had new run IDs. Full table-row
hashes, including accepted Kafka lineage, stayed unchanged after replay. The
weather canonical table gained a physical overwrite snapshot; the other four
snapshot IDs stayed unchanged. Both weather sinks independently read the same
one Kafka record. They do not represent two source publications.

The archive contains real data collected on November 24. This check made no new
source API requests and created no AWS resources. No original topic, checkpoint,
producer state or lake file was reset or deleted.

## Acceptance Boundary

This tests graceful broker and Spark process restarts in a local Linux container,
including pending input across a broker restart. It is not a host reboot or
power-loss simulation. The probe runs as root; EC2 `ubuntu` permissions, Compose
service startup, cloud-init, systemd activation and the shutdown timer still need
on-host checks. Disk loss and infrastructure replacement require their own
recovery policy; this single-host design provides no replicated storage.

An identical replay may create a physical Iceberg overwrite snapshot without
changing logical rows. Only the no-input phase requires stable snapshot IDs.
See the [weather replay evidence](weather-correction-verification.md). The
existing table/checkpoint contracts are unchanged; no migration is required.
