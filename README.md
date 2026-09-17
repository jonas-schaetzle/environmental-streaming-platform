# Environmental Streaming Platform

Cloud-oriented streaming data platform for heterogeneous environmental data.

The project is intentionally built step by step. It currently demonstrates a local Spark and Kafka streaming path before moving toward cloud infrastructure, Iceberg, observability, and production-style platform concerns.

## Current Phase

Local Kafka and Spark Structured Streaming foundation.

The repository currently contains:

- OpenAQ ingestion scripts for local source snapshots
- batch transformations into a canonical measurement model
- file-based Spark Structured Streaming examples
- local Apache Kafka via Docker Compose
- Python Kafka producer and consumer examples
- direct OpenAQ-to-Kafka producer with duplicate suppression
- JSONL export for canonical measurement events
- Spark Structured Streaming from Kafka to valid and quarantine Parquet outputs
- canonical unit normalization for measurement outputs
- validation and quality-report helpers
- focused pytest coverage for transformation and Kafka helper logic

## Local Setup

Install Python dependencies:

```bash
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

Spark 4.2.0 requires Java 17, 21, or 25. The project tries to auto-detect a local Java runtime before creating Spark sessions, including common Homebrew JDK paths on macOS.

On macOS with Homebrew, Java 21 can be installed with:

```bash
brew install openjdk@21
```

If Java is installed but Spark still cannot find it, set `JAVA_HOME` explicitly:

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
```

Start local Kafka:

```bash
docker compose up -d
```

Create the local raw measurement topic if it does not exist yet:

```bash
docker exec environmental-streaming-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic environment.measurements.raw --partitions 3 --replication-factor 1
```

Export canonical OpenAQ measurements from Parquet to JSONL:

```bash
python src/canonical_jsonl_export.py --input-path data/output/openaq_location_latest --output-path data/stream/input/openaq_location_latest.jsonl
```

Produce canonical measurement events to Kafka:

```bash
python src/kafka_measurement_producer.py --input-path data/stream/input/openaq_location_latest.jsonl --topic environment.measurements.raw --bootstrap-servers localhost:9092
```

Fetch current OpenAQ measurements and produce new sensor timestamps directly to Kafka:

```bash
python src/openaq_kafka_producer.py --location-id 2178
```

Use a polling interval to keep the producer running. The local state file prevents an
unchanged latest measurement from being published repeatedly:

```bash
python src/openaq_kafka_producer.py --location-id 2178 --poll-interval-seconds 300
```

Consume messages for debugging:

```bash
python src/kafka_measurement_consumer.py
```

Run the Spark Kafka stream:

```bash
python src/measurement_kafka_stream.py
```

The Kafka stream writes valid events to `data/stream/output/kafka_canonical_measurements` and invalid events to `data/stream/output/kafka_invalid_measurements`. Invalid events keep Kafka metadata such as topic, partition, offset, key, and raw value for traceability.

Valid measurement outputs normalize equivalent unit spellings, for example `ug/m3` to `µg/m³`.

Generated local outputs are written under:

- `src/` for application code
- `tests/` for tests
- `data/output/` for batch outputs
- `data/stream/output/` for streaming outputs
- `data/stream/checkpoints/` for Spark checkpoints

## Near-Term Direction

Next steps:

- introduce event-time windowing on Kafka input
- write curated streaming results to Apache Iceberg tables
- prepare the lakehouse layer and cloud deployment path

## Canonical Measurement Model

The platform normalizes source-specific environmental measurements into a small canonical model before writing analytical outputs.

Current fields:

- `source`: source system name, for example `openaq`
- `location_id`: source-specific location identifier
- `sensor_id`: source-specific sensor identifier
- `parameter`: measured quantity, for example `pm25`, `no2`, or `o3`
- `parameter_display_name`: human-readable parameter name
- `value`: measured numeric value
- `unit`: measurement unit
- `measured_at_utc`: event timestamp in UTC
- `latitude`: measurement latitude
- `longitude`: measurement longitude

This model is intentionally small and will evolve as additional sources and streaming semantics are added.
