# Event-Time Verification

The verification follows one PM2.5 hour through the existing Kafka, Spark and
Iceberg code. It reads a real archived OpenAQ record from the local canonical
table and injects three controlled observations based on it. It uses a separate
Kafka broker, topic, Iceberg warehouse and pair of checkpoints. The archived
warehouse is read through a separate catalog alias.

| Step | Expected result |
|---|---|
| Publish an observation 40 minutes into the hour | The hour remains open |
| Publish the original observation out of order, plus a later event that advances the watermark beyond the target hour | The finalized hour contains both observations |
| Send another observation for the closed hour | The canonical table retains the record and its Kafka identity. The hourly query reports a row dropped by its watermark and the finalized hour retains its original Iceberg snapshot |
| Recalculate the hour from canonical rows in a batch read | The calculation includes all three records and shows the impact of the late arrival |
| Start both queries again with their checkpoints | Query IDs persist, run IDs change, and with zero new input both table snapshots stay the same |

The two-hour watermark advances with the latest processed event time. A closed
hour has a stable streaming result, while its canonical records can support a
reviewed historical recomputation. The verification does not write the
recalculated result. See [Spark Structured Streaming](https://spark.apache.org/docs/4.1.1/streaming/apis-on-dataframes-and-datasets.html#handling-late-data-and-watermarking).

## Run Locally

Use the Python and Java setup from the [README](../README.md#local-setup) and a
local warehouse containing an archived OpenAQ PM2.5 observation at a UTC hour
boundary. Start a separate Kafka container with the same image as Compose. Pick
an unused container name and keep port 19092 reserved for the verification.

```bash
mkdir -p data/verification
drill_broker_data="$(mktemp -d "$PWD/data/verification/event_time_broker.XXXXXX")"
docker run -d --name environmental-streaming-event-time \
  --publish 127.0.0.1:19092:9092 \
  --mount "type=bind,source=$drill_broker_data,target=/var/lib/kafka/data" \
  --env KAFKA_NODE_ID=1 --env KAFKA_PROCESS_ROLES=broker,controller \
  --env KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  --env KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:19092 \
  --env KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  --env KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
  --env KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  --env KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  --env KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
  --env KAFKA_LOG_DIRS=/var/lib/kafka/data apache/kafka:4.2.1
```

On Linux, the bind mount must be writable by Kafka's container user. See
[Local Kafka Storage And Restart](local-kafka.md#fresh-setup). Once Kafka accepts
connections from the host, run the verification and stop its broker:

```bash
python scripts/verify_event_time.py
docker stop environmental-streaming-event-time
```

The script prints the path to `report.json` under ignored `data/verification/`.
It records the comparison, Kafka identity, Iceberg snapshots and streaming
progress. Every invocation creates a fresh run directory and topic. The test
covers bounded query restarts on the local machine. Cloud host reboot and
recovery after an interrupted write have separate acceptance criteria.

## Verified Result

On 2026-11-10, the local run traced Munich Stachus PM2.5 for 12:00–13:00 UTC on
2026-11-04. The finalized window included two observations, averaging
16.725 µg/m³. Spark reported one row dropped by the hourly watermark. The
canonical table retained that row at partition 0, offset 3. A read-only
recalculation included all three observations and averaged 24.483 µg/m³.
Both queries resumed their checkpoints with zero new input and kept the
Iceberg snapshots observed after processing the late record.

The initial run exposed empty Iceberg MERGE commits during watermark progress.
The hourly sink now checks the cached batch before merging. In the verified
run, the late input advanced only the canonical snapshot and the finalized
hour kept its prior snapshot. The check changes write behavior for empty
hourly batches. The aggregation, table schema and checkpoint format stay
compatible with existing data.
