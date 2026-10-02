# Environmental Streaming Platform

[![CI](https://github.com/jonas-schaetzle/environmental-streaming-platform/actions/workflows/ci.yml/badge.svg?branch=dev)](https://github.com/jonas-schaetzle/environmental-streaming-platform/actions/workflows/ci.yml)

Production-oriented streaming data platform for real environmental measurements.
The current implementation continuously ingests OpenAQ data, publishes canonical
measurement events to Kafka, processes them with Spark Structured Streaming, and
stores canonical, quarantined, and hourly aggregate data in Apache Iceberg.

## Project Documentation

- [Architecture](docs/architecture.md)
- [Roadmap](docs/roadmap.md)
- [Development Workflow](docs/development-workflow.md)

## Architecture

```text
OpenAQ API
    |
    v
OpenAQ Kafka Producer
    |
    v
environment.measurements.canonical
    |
    v
Spark Structured Streaming
    |-- valid events ------> local.lake.canonical_measurements
    |-- hourly aggregates -> local.lake.hourly_measurement_aggregates
    `-- invalid events ----> local.lake.quarantine_measurements
```

Kafka offsets and Spark checkpoints make the processing restartable. OpenAQ
measurements are keyed by sensor ID, and the producer persists the latest published
timestamp per sensor to avoid publishing unchanged API results repeatedly.

## Package Structure

```text
src/environmental_streaming/
    ingestion/    # external source clients and producers
    messaging/    # Kafka publishing adapters
    processing/   # canonical models and Spark streaming pipelines
    lakehouse/    # Iceberg table definitions, audit, and maintenance
    products/     # read-only analytical products
    diagnostics/  # operational inspection tools
    runtime/      # Spark and Java runtime configuration
```

Run command-line modules with `python -m environmental_streaming...` after the
editable installation described below. This keeps imports package-safe while source
changes remain immediately available in the virtual environment.

## Current Capabilities

- direct ingestion from the OpenAQ API for one or more locations
- curated configuration for Munich, Stuttgart, and Hamburg stations
- bounded retry with exponential backoff for temporary OpenAQ failures
- per-location failure isolation during multi-location polling
- canonical environmental measurement contract
- duplicate suppression across producer restarts
- cached sensor metadata during continuous polling
- Kafka delivery verification and sensor-based partition keys
- Spark Structured Streaming with checkpointed Kafka offsets
- canonical, quarantine, and hourly aggregate Apache Iceberg tables
- idempotent Iceberg event writes keyed by Kafka topic, partition, and offset
- replay-safe aggregate upserts keyed by window and measurement dimensions
- read-only Iceberg maintenance reporting with explicit compaction and snapshot
  expiration actions
- hourly event-time aggregates with a two-hour late-data watermark
- location-level JSON insights with latest values, finalized hourly statistics, and
  configurable data-freshness status
- unit normalization for particulate measurements
- validation with a separate quarantine output and Kafka trace metadata
- local Kafka runtime through Docker Compose

## Local Setup

Create and activate a Python virtual environment, then install the dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m pip install --no-deps -e .
```

Create the local environment file, add an OpenAQ API key, and load it into the
current shell:

```bash
cp .env.example .env
set -a
source .env
set +a
```

Spark 4.1.1 requires Java 17 or 21. The project auto-detects common Java
installations, including Homebrew JDK paths on macOS. Java 21 can be installed with:

```bash
brew install openjdk@21
```

Start Kafka:

```bash
docker compose up -d
```

Create the canonical measurement topic once:

```bash
docker exec environmental-streaming-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic environment.measurements.canonical \
  --partitions 3 \
  --replication-factor 1
```

## Running The Pipeline

Run one ingestion cycle for the curated locations in
`config/openaq_locations.json`:

```bash
python -m environmental_streaming.ingestion.openaq_producer
```

Poll the configured locations continuously:

```bash
python -m environmental_streaming.ingestion.openaq_producer \
  --poll-interval-seconds 300
```

Each cycle writes one JSON report containing location coverage, fetched and published
event counts, the latest measurement timestamp, freshness in seconds, and any
location-specific request error. A temporary failure at one location does not block
the remaining locations or stop continuous polling.

Repeat `--location-id` to override the location file for an ad hoc run:

```bash
python -m environmental_streaming.ingestion.openaq_producer \
  --location-id 2669 \
  --location-id 2936
```

In another terminal, process all available Kafka events:

```bash
python -m environmental_streaming.processing.measurement_stream
```

Valid measurements are aggregated into one-hour event-time windows per source,
location, parameter, and unit. Each finalized window contains the measurement count,
average, minimum, maximum, and latest measurement timestamp. Spark waits up to two
hours of event time for late measurements. Older events remain in the canonical sink
but no longer update a finalized aggregate window.

Canonical measurements are written to `local.lake.canonical_measurements` in the
local Iceberg warehouse. Each micro-batch is merged by Kafka topic, partition, and
offset, so records imported before the stream starts or replayed after a restart are
not inserted twice.
Invalid records follow the same replay-safe merge strategy in
`local.lake.quarantine_measurements`. Their hidden daily partition uses the Kafka
timestamp because malformed payloads may not contain a usable measurement timestamp.
Finalized hourly windows are merged into
`local.lake.hourly_measurement_aggregates` using their window, source, location,
parameter, and unit as the business key. A replay inserts new windows and updates
existing windows with recalculated metrics instead of creating duplicates.

The aggregate checkpoint owns the window state. Changing the window duration,
watermark delay, or grouping keys requires a deliberate new checkpoint and a
documented rebuild or migration of the Iceberg aggregate table; do not delete or
reuse the existing checkpoint implicitly.

Inspect Kafka events without committing consumer offsets:

```bash
python -m environmental_streaming.diagnostics.kafka_consumer
```

Audit all Iceberg tables after a pipeline run or backfill:

```bash
python -m environmental_streaming.lakehouse.audit
```

The JSON report includes row and data-file counts, duplicate or incomplete identity
keys, hidden partitioning, freshness timestamps, and snapshot metadata. The command
returns a non-zero exit code when any table fails its identity or partition checks.

Inspect small-file and snapshot maintenance needs without changing the tables:

```bash
python -m environmental_streaming.lakehouse.maintenance
```

The report evaluates small-file and file-count compaction triggers within each hidden
partition and shows how many old snapshots are eligible under the configured
retention policy. Maintenance only runs when explicitly requested. Compact all known
tables with Iceberg bin-packing:

```bash
python -m environmental_streaming.lakehouse.maintenance --compact
```

Expire snapshots older than seven days while always retaining the five most recent
snapshots per table:

```bash
python -m environmental_streaming.lakehouse.maintenance --expire-snapshots
```

Use `--table canonical`, `--table quarantine`, or `--table hourly` to restrict an
operation. Compaction preserves logical rows but creates a new snapshot. Snapshot
expiration removes old time-travel history and unreferenced files; it does not alter
Kafka offsets or Spark checkpoints.

Generate a current report for every curated location from the Iceberg tables:

```bash
python -m environmental_streaming.products.air_quality_insights
```

The report contains the latest value per parameter, its measurement age, and the
latest finalized hourly average, minimum, maximum, and count. Configured locations
without measurements remain visible with the status `missing`. Measurements are
`fresh` for up to two hours by default and `stale` afterwards. Override that
operational freshness threshold when needed:

```bash
python -m environmental_streaming.products.air_quality_insights \
  --freshness-threshold-minutes 60
```

These statuses describe data availability, not health risk or regulatory air-quality
classification.

All runtime state, checkpoints, and measurement outputs live under `data/` and are
excluded from version control. Legacy Parquet outputs and checkpoints may remain
there after the Iceberg cutover, but the pipeline no longer updates or deletes them.

## Verification

```bash
ruff check .
pytest
```

GitHub Actions runs the same checks with Python 3.11 and Java 21 on pushes to
`dev` and `main`, and on release pull requests targeting `main`.

## Data Model

The canonical event contract, Kafka trace fields, Iceberg merge identities,
partitioning, and state semantics are documented in
[Architecture](docs/architecture.md).

The weather model in `processing/weather_model.py` provides a tested contract for
hourly model data with parameter-specific validation, explicit UTC timestamps, and
invalid-event reasons while preserving input lineage. Weather source ingestion,
Kafka publishing, and Iceberg storage are planned and do not run yet.

## Roadmap

Phases 1 and 2 are complete. Phase 3 is in progress: the next outcome is weather
ingestion followed by temporal and spatial enrichment of air-quality observations.
See the maintained [Roadmap](docs/roadmap.md) for completed capabilities, upcoming
work, and phase completion criteria.
