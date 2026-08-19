# Environmental Streaming Platform

Cloud-oriented streaming data platform for heterogeneous environmental data.

The project is intentionally built step by step. The first goal is to learn and demonstrate Apache Spark fundamentals locally before adding Kafka, cloud infrastructure, Iceberg, observability, and production-style platform concerns.

## Current Phase

Phase 0: local development setup.

This repository currently contains only the minimal structure needed to begin:

- `data/input/` for small local input files
- `data/output/` for generated local outputs
- `src/` for application code
- `tests/` for tests
- `requirements.txt` and `pyproject.toml` as intentionally minimal dependency and project configuration placeholders

## Near-Term Direction

The next implementation step is a small Spark batch job over local environmental sample data. That step will introduce Spark deliberately, with explicit schemas, DataFrame transformations, actions, and Parquet output.

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