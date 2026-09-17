# Environmental Streaming Platform

Production-oriented streaming data platform for real environmental measurements.
The current implementation continuously ingests OpenAQ data, publishes canonical
measurement events to Kafka, validates them with Spark Structured Streaming, and
separates usable data from quarantined records.

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
    |-- valid events ------> data/lake/canonical_measurements
    `-- invalid events ----> data/lake/quarantine_measurements
```

Kafka offsets and Spark checkpoints make the processing restartable. OpenAQ
measurements are keyed by sensor ID, and the producer persists the latest published
timestamp per sensor to avoid publishing unchanged API results repeatedly.

## Current Capabilities

- direct ingestion from the OpenAQ API for one or more locations
- canonical environmental measurement contract
- duplicate suppression across producer restarts
- cached sensor metadata during continuous polling
- Kafka delivery verification and sensor-based partition keys
- Spark Structured Streaming with checkpointed Kafka offsets
- unit normalization for particulate measurements
- validation with a separate quarantine output and Kafka trace metadata
- local Kafka runtime through Docker Compose

## Local Setup

Create and activate a Python virtual environment, then install the dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Create the local environment file, add an OpenAQ API key, and load it into the
current shell:

```bash
cp .env.example .env
set -a
source .env
set +a
```

Spark 4.2.0 requires Java 17, 21, or 25. The project auto-detects common Java
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

Run one ingestion cycle for an OpenAQ location:

```bash
python src/openaq_kafka_producer.py --location-id 2178
```

Repeat `--location-id` to ingest multiple locations, or poll continuously:

```bash
python src/openaq_kafka_producer.py \
  --location-id 2178 \
  --poll-interval-seconds 300
```

In another terminal, process all available Kafka events:

```bash
python src/measurement_kafka_stream.py
```

Inspect Kafka events without committing consumer offsets:

```bash
python src/kafka_measurement_consumer.py
```

All runtime state, checkpoints, and measurement outputs live under `data/` and are
excluded from version control.

## Verification

```bash
ruff check .
pytest
```

## Canonical Measurement Contract

- `source`: source system, currently `openaq`
- `location_id`: source-specific location identifier
- `sensor_id`: source-specific sensor identifier
- `parameter`: measured quantity such as `pm25`, `no2`, or `o3`
- `parameter_display_name`: human-readable parameter name
- `value`: numeric measurement value
- `unit`: normalized measurement unit
- `measured_at_utc`: event timestamp in UTC
- `latitude`: measurement latitude
- `longitude`: measurement longitude

## Roadmap

- ingest a curated set of real locations continuously
- add event-time windows and late-event handling on Kafka input
- replace the Parquet sink with Apache Iceberg tables
- enrich measurements with weather data
- expose air-quality trends, anomalies, and data-freshness metrics
- add operational monitoring and cloud deployment
