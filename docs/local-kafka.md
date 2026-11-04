# Local Kafka Storage And Restart

The Compose broker uses the pinned `apache/kafka:4.2.1` image and explicitly sets
`KAFKA_LOG_DIRS=/var/lib/kafka/data`. That directory is bind-mounted from ignored
`data/kafka/`, including Kafka records, KRaft metadata and committed consumer-group
offsets. This is independent of producer state and Spark checkpoints.

Port 9092 is published only on `127.0.0.1` and `::1`. Both loopback families are
bound so existing `localhost:9092` clients work with either address preference.
Host-based producers and Spark continue to use `localhost:9092`; remote access and container-to-container clients need an
explicit listener design. Do not expose this plaintext broker to the internet.

## Fresh Setup

Use this only when no existing broker data needs preservation. If a broker already
exists from the earlier configuration, follow the migration section first.

```bash
mkdir -p data/kafka
docker compose up -d --wait --wait-timeout 120 broker
```

Compose does not silently create a missing bind directory (`create_host_path:
false`). A deliberately initialized empty directory creates a new Kafka cluster;
never use that to repair an existing environment. The image's anonymous volume
alone does not establish where Kafka actually writes its logs.

The health check lists topics through the broker API. `--wait` therefore checks
broker readiness, not merely that a Java process is running. It does not certify
source freshness, downstream processing or complete data coverage.

On Linux, ensure the directory is writable by the image's `appuser` before
starting. Inspect the pinned image's numeric user/group and set ownership only on
the intended data directory; do not use world-writable permissions. Host paths
must remain stable when deploying a new checkout or moving the project.

## Migrating The Previous Broker

The earlier Compose configuration had no explicit `KAFKA_LOG_DIRS`. Inspection of
the existing local broker on 2026-11-03 found its active data at `/tmp/kafka-logs`
inside the container, while the image's `/var/lib/kafka/data` anonymous volume was
not the active log directory. Do not infer data location from mounts alone.

Do not run the updated `docker compose up` against an old container before the
copy is complete: Compose replaces the container when applying the new mounts.

1. Pause all producers and Spark writers. Confirm the intended container, image,
   configured log directories and any separate `metadata.log.dir`. Custom setups
   require their own complete copy plan. Keep the same Kafka version and node ID.
2. Capture cluster ID, topic IDs, topic list, earliest/end offsets and checksums of
   retained records through a reader with auto commit disabled. Record producer
   state/checkpoint file hashes. A stopped broker can first be backed up without
   starting it; normal Kafka retention may remove expired segments on startup.
3. Stop the broker cleanly. Keep an exclusive copy of the complete log directory
   under `data/verification/<run>/`, including hidden files and KRaft metadata.
   Never copy a running broker as a consistent migration snapshot.
4. Confirm `data/kafka` does not exist. Copy the stopped broker's entire active
   data directory there. For the inspected default configuration:

   ```bash
   docker stop --timeout 60 environmental-streaming-kafka
   docker cp environmental-streaming-kafka:/tmp/kafka-logs data/kafka
   ```

   These commands assume the target is absent and all writers are paused. Do not
   merge directories, overwrite an existing target, or copy only topic partitions.
   If a separate metadata directory exists, stop and adapt the migration plan.
5. Compare the copied tree's relative paths and file hashes with a second copy
   from the same stopped container. Confirm `meta.properties` retains the cluster,
   node and directory identity. Keep the backup and container until this passes.
6. Apply the updated Compose configuration:

   ```bash
   docker compose up -d --wait --wait-timeout 120 broker
   ```

   This replaces the old container; the verified data copy and backup preserve
   its state. Check the live mount source and effective `log.dirs`, then compare
   cluster/topic IDs, offsets and retained payload hashes with the baseline.
7. Restart the broker and repeat the comparison. Recreate the container once more
   using the same bind directory and repeat again. Producer-state and checkpoint
   hashes must remain unchanged while their writers are paused.

Keep the backup after verification. Recovery must use a complete consistent state
set; do not restore an arbitrary older Kafka copy while advancing existing producer
state or checkpoints. Never run two brokers against the same directory, delete
`meta.properties`, reformat the data, prune volumes, or reset checkpoints to make
a migration pass. If verification fails, stop writers and investigate before
resuming collection. Re-creation is not a substitute for host-reboot verification.

## Normal Operation And Data Limits

```bash
docker compose ps
docker compose restart broker
```

Wait for the health check to return healthy before running ingestion or Spark.
`docker compose up -d --wait --wait-timeout 120 broker` provides an explicit
readiness wait after restart as well.

Container restart and replacement retain the bind directory. Docker/host storage
loss, disk exhaustion and a deleted directory remain failure scenarios: this
single-broker setup provides neither replication nor a backup policy. Existing
Kafka retention still applies; persistent storage does not retain every event
forever. Monitor disk usage during local collection and bound cloud lifetimes.

If Kafka has expired input needed by a Spark checkpoint, processing may fail rather
than silently skip it. Investigate retained offsets and source availability;
catch-up/backfill requires a separate decision. Never set `failOnDataLoss=false`,
remove producer state or reset checkpoints as a repair shortcut.

## Verification Evidence

Local evidence is stored under ignored
`data/verification/kafka_persistence_20261103/`. The maintained summary below
separates broker persistence from full pipeline and host-reboot acceptance.

Verified on Docker Desktop/macOS on 2026-11-03:

| Check | Result |
|---|---|
| Stopped-broker copy | All 345 copied files matched the second-copy hashes |
| Migration, container recreation and broker restart | Same cluster ID, topic list, canonical topic IDs, earliest/end offsets and payload checksums |
| Retained real input | Nine new OpenAQ events and 72 weather events preserved |
| External progress during broker changes | All 156 captured producer-state/checkpoint files unchanged |
| API coverage | All three configured locations responded for each source; this does not imply fresh station data |
| Real Spark path | Measurement canonical/quarantine/hourly jobs and both weather sinks completed |
| Iceberg audit | All five tables healthy, with zero duplicate or incomplete identities |
| Second Spark run without new Kafka input | Same row counts, file counts and snapshot IDs for all five tables |
| Repository checks | Ruff passed; all 95 tests passed; Compose configuration valid |

After processing, table row counts were 35 canonical measurements, zero measurement
quarantine rows, 26 finalized hourly aggregates, 72 canonical weather rows and six
weather quarantine rows. The six weather quarantine rows predate this run; no new
invalid input was injected. This check proves checkpoint continuation with no new
input, not republishing corrections at new offsets or arbitrary outage recovery.

Source freshness remains a product concern: the Hamburg OpenAQ station's newest
reported time was 2026-09-21T08:00:00Z despite a successful response on November 3.
The latest Munich/Stuttgart observations were 2026-11-03T12:00:00Z. Current-hour
weather was present for all three configured reference locations. Finalized hourly
air-quality output still depends on later event-time input advancing its watermark;
successful ingestion alone does not make every recent hourly window available.

At the time of the 2026-11-03 check, the audit serialized collected Python
datetimes without a timezone suffix.
The first local audit displayed host-local times; the final comparison ran report
processes with `TZ=UTC` to make their timestamps comparable:

```bash
TZ=UTC python -m environmental_streaming.lakehouse.audit --source all
```

Row counts, identities and snapshots were identical across both audits; timestamp
display differed with the process timezone. The subsequent UTC report fix removes
the need for this workaround: audit timestamps have explicit `Z` suffixes and
report ages use actual instants independently of the host timezone. See the
[report timestamp boundary](architecture.md#report-timestamp-boundary) for output
compatibility and regeneration of earlier reports. No stored event time or source
contract was changed by either package.

Still unverified: host reboot, fresh Linux deployment/permissions, disk-loss recovery,
extended polling-gap catch-up, and the broader weather correction/replay cases in
the roadmap. Existing tests cover model/revision rules, but this run does not
replace their broader real-path acceptance evidence. No checkpoint, producer state,
topic or lake data was reset; the old Kafka data copies remain under data/.
