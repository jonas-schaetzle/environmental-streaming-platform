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
