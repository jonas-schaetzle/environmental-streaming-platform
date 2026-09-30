# Architecture

## Purpose And Scope

The Environmental Streaming Platform turns real environmental observations into
restartable, auditable analytical data products. The current production path uses
OpenAQ as its source, Kafka as the event backbone, Spark Structured Streaming for
validation and event-time processing, and Apache Iceberg for analytical storage.

The platform is local-first today, but its state, contracts, and operational
boundaries are designed to remain explicit when the system moves to managed
infrastructure.

## Current Data Flow

```text
OpenAQ API
    |
    v
OpenAQ producer ----> producer state
    |
    v
environment.measurements.canonical
    |
    v
Spark Structured Streaming
    |-- valid events ------> local.lake.canonical_measurements
    |-- invalid events ----> local.lake.quarantine_measurements
    `-- hourly windows ----> local.lake.hourly_measurement_aggregates
                                      |
                                      v
                         air-quality insights

Iceberg tables <---- audit and explicit maintenance commands
```

The producer can poll continuously. The Spark job currently uses an
`availableNow` trigger, processes all available Kafka records, commits its
checkpoints, and stops.

## Component Boundaries

| Package | Responsibility |
|---|---|
| `ingestion` | OpenAQ HTTP access, polling, metadata enrichment, and source-side deduplication |
| `messaging` | Kafka serialization, sensor-based keys, and delivery verification |
| `processing` | Canonical measurement rules, validation, normalization, and Spark stream orchestration |
| `lakehouse` | Iceberg table contracts, health audits, compaction, and snapshot expiration |
| `products` | Read-only analytical outputs derived from trusted Iceberg tables |
| `diagnostics` | Operational inspection tools that do not mutate processing state |
| `runtime` | Local Spark, Iceberg catalog, and Java runtime configuration |

The exact module ownership rules are maintained in `AGENTS.md`. External source
clients, transport adapters, domain transformations, and storage operations stay
separate so that a new source does not silently change the canonical contract or
the behavior of an existing sink.

## Event Contract

The canonical Kafka payload contains the following fields:

| Field | Meaning |
|---|---|
| `source` | Source system, currently `openaq` |
| `location_id` | Source-specific monitoring location identifier |
| `sensor_id` | Source-specific sensor identifier |
| `parameter` | Measured quantity such as `pm25`, `no2`, or `o3` |
| `parameter_display_name` | Human-readable parameter name |
| `value` | Numeric measurement value |
| `unit` | Measurement unit normalized during processing |
| `measured_at_utc` | Observation time in UTC |
| `latitude` | Monitoring location latitude |
| `longitude` | Monitoring location longitude |

Spark preserves the Kafka topic, partition, offset, timestamp, key, and raw value
next to the parsed payload. This trace metadata is part of both the canonical and
quarantine records and connects every stored row to its transport origin.

## Iceberg Tables

| Table | Purpose | Merge identity | Hidden partition |
|---|---|---|---|
| `local.lake.canonical_measurements` | Valid normalized observations | Kafka topic, partition, and offset | Day of `measured_at_utc` |
| `local.lake.quarantine_measurements` | Invalid observations with a validation reason | Kafka topic, partition, and offset | Day of `kafka_timestamp` |
| `local.lake.hourly_measurement_aggregates` | Finalized hourly statistics | Window, source, location, parameter, and unit | Day of `window_start` |

The canonical and quarantine tables use append-only merge behavior: a known Kafka
identity is not inserted again. The aggregate table updates a known business key
with recalculated count, average, minimum, maximum, and latest observation time.

All tables use Iceberg format version 2 with Parquet data files. The local Hadoop
catalog is named `local`, and its warehouse is stored under `data/warehouse`.

## Time And State Semantics

- OpenAQ observations are keyed in Kafka by sensor ID.
- Producer state records the latest published timestamp per sensor and suppresses
  unchanged API results across producer restarts.
- Spark starts from Kafka's earliest offsets only when no checkpoint exists.
  Existing checkpoints determine continuation after a restart.
- Canonical, quarantine, and aggregate queries own separate checkpoints under
  `data/checkpoints/`.
- One-hour aggregates use `measured_at_utc` as event time and a two-hour watermark.
  Events older than the retained watermark can still enter the canonical table but
  no longer update a finalized aggregate window.
- `foreachBatch` writes use Iceberg `MERGE` operations. Replayed Kafka input is
  therefore safe against duplicate table rows for the defined identities.

This design does not rely on a broad exactly-once claim. It combines checkpointed
source progress with deterministic identities and idempotent sink operations.

Changing a payload schema, normalization rule, merge identity, event-time window,
watermark, grouping key, or checkpoint path requires an explicit compatibility and
migration decision. Existing checkpoints or lake data must never be deleted merely
to make a changed job start.

## Data Quality And Operations

Malformed or invalid events are routed to quarantine with a machine-readable
validation reason and their Kafka trace metadata. Unit normalization is applied to
valid particulate measurements before they reach the canonical table.

Operational tools have deliberately narrow permissions:

- `environmental_streaming.lakehouse.audit` reads table health, identity
  completeness, duplicates, partitions, freshness, files, and snapshots.
- `environmental_streaming.lakehouse.maintenance` reports maintenance needs by
  default. Compaction and snapshot expiration require explicit command flags.
- `environmental_streaming.diagnostics.kafka_consumer` never commits consumer
  offsets.
- `environmental_streaming.products.air_quality_insights` reads trusted tables and
  reports current values, finalized hourly statistics, and data freshness without
  modifying lake state.

Secrets belong in environment variables or the ignored `.env` file. Producer
state, Spark checkpoints, Iceberg data, and downloaded measurements belong under
`data/` and remain untracked.

## Planned Extension

Weather ingestion is the next architectural extension. It will enter through a
source-specific ingestion boundary and receive an explicit storage contract before
air-quality and weather observations are joined by location and time. A second
stream processor such as Flink is not planned unless a concrete low-latency use
case justifies the additional runtime and state model.
