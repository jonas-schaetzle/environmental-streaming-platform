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

## Weather Source Check

Capture hourly weather for one configured location using the public non-commercial
Open-Meteo endpoint:

```bash
python -m environmental_streaming.ingestion.weather_client \
  --weather-location-id munich
```

Use `stuttgart` or `hamburg` for the other curated locations. The command requires
no OpenAQ key or running Kafka/Spark services. By default it requests the previous
three hours plus the current hour, in UTC with Celsius, metres-per-second wind
speed, and millimetre precipitation. Use `--past-hours 6` to expand the recent range
or `--locations-file path/to/weather_locations.json` for another location file.

Each successful capture creates a timestamped JSON file under `data/weather/` and
prints a JSON report containing the location ID, fetch time, hour count, and file
path. The snapshot preserves requested coordinates, station mappings, source and
dataset labels, the UTC response-receipt time, and the decoded API response.
Existing captures are never overwritten. Convert a saved capture into canonical
JSON Lines events (replace the example filename with the reported capture path):

```bash
python -m environmental_streaming.ingestion.weather_events \
  --capture-file "data/weather/open_meteo_<timestamp>.json"
```

The export creates `<capture-name>_canonical.jsonl` under `data/weather/` without
overwriting existing files. Each line contains one parameter for one UTC hour,
with canonical units, requested and returned grid coordinates, and the original
receipt timestamp. Future hours relative to that timestamp are excluded. The
export uses the saved location metadata, not today's location configuration.
Missing or invalid individual values remain unchanged for downstream validation;
an export is not a guarantee of valid data. Invalid time labels, duplicate hours,
or broken response envelopes fail the conversion before a file is created.
Neither command publishes to Kafka or changes producer state or checkpoints.

### Weather Kafka Replay

Start the local Kafka broker with `docker compose up -d` and create the separate
weather topic once (existing topics are left intact):

```bash
docker exec environmental-streaming-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic environment.weather.canonical --partitions 3 --replication-factor 1

python -m environmental_streaming.ingestion.weather_replay \
  --events-file "data/weather/open_meteo_<timestamp>_canonical.jsonl"

python -m environmental_streaming.diagnostics.kafka_consumer \
  --topic environment.weather.canonical --max-messages 24 \
  --group-id weather-replay-check
```

The replay command checks the entire file for JSON objects, supported
source/dataset identities, non-empty location IDs, and the presence of both
original timestamps before sending anything. It preserves the event payloads,
including receipt times and invalid individual values for future quarantine.
Domain validation still belongs to the weather model. Records use the key
`open_meteo:<weather_location_id>`; success is reported only after Kafka delivery
callbacks complete without errors and no records remain queued within 30 seconds.
Use `--bootstrap-servers host:port` to publish to another broker.

Replay is explicit and has no persistent deduplication state. Running it again,
or retrying after partial delivery, can create new Kafka offsets for the same
business keys. Keys group events by location; they do not deduplicate records.
The diagnostic reader starts at the earliest available offsets for its group and
never commits offsets, so its output can include earlier replays. No producer
state or Spark checkpoints are changed. Automated polling, correction-aware
deduplication, weather Spark sinks, and Iceberg storage remain planned.

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
invalid-event reasons while preserving input lineage. The Open-Meteo HTTP client
supports raw source checks, and `ingestion/weather_events.py` converts responses
and saved captures into canonical event payloads. Saved events can be replayed to
the dedicated weather Kafka topic; automated polling and weather Iceberg storage
are planned and do not run yet.

[Weather location configuration](config/weather_locations.json) defines fixed city
reference coordinates for Munich, Stuttgart, and Hamburg and maps the curated
OpenAQ stations to those locations. The loader in `ingestion/weather_locations.py`
validates coordinates and prevents duplicate or ambiguous mappings. These points
provide city-level context rather than weather measured at the OpenAQ stations.

## Roadmap

Phases 1 and 2 are complete. Phase 3 is in progress: the next outcome is weather
ingestion followed by temporal and spatial enrichment of air-quality observations.
See the maintained [Roadmap](docs/roadmap.md) for completed capabilities, upcoming
work, and phase completion criteria.
