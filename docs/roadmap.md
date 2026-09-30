# Roadmap

## Product Vision

Build a production-oriented environmental streaming platform that continuously
turns real observations into explainable, current information for analysis and
alerting.

```text
real sources -> reliable events -> governed lakehouse data -> useful products
```

The primary product direction is a live air-quality monitor for multiple cities
and monitoring locations. It should make current pollution, trends, data freshness,
and unusual changes visible while preserving the lineage needed to investigate bad
or missing data.

## Phase Overview

| Phase | Outcome | Status |
|---|---|---|
| Phase 0 | Stable real-data foundation | Complete |
| Phase 1 | Resilient multi-location ingestion | Complete |
| Phase 2 | Event-time analytics and Iceberg lakehouse | Complete |
| Phase 3 | Environmental insights and alerts | In progress |
| Phase 4 | Observable cloud operation | Planned |

## Phase 0 - Foundation

Delivered:

- a real OpenAQ-to-Kafka-to-Spark path without tutorial-only pipelines
- a canonical environmental measurement contract
- explicit validation, unit normalization, and quarantine behavior
- local Kafka and Spark runtime support
- persistent producer and streaming state

## Phase 1 - Real Multi-Location Data

Delivered:

- curated OpenAQ locations in Munich, Stuttgart, and Hamburg
- one-shot and continuous polling
- bounded retry with exponential backoff for transient source failures
- per-location failure isolation and source coverage reporting
- cached sensor metadata and restart-safe source deduplication

## Phase 2 - Streaming Analytics And Lakehouse

Delivered:

- one-hour event-time windows with a two-hour late-data watermark
- canonical, quarantine, and hourly aggregate Iceberg tables
- idempotent measurement writes based on Kafka identity
- replay-safe aggregate upserts based on the window business key
- read-only table and backfill auditing
- explicit data-file compaction and snapshot-expiration workflows
- a package structure that separates ingestion, processing, lakehouse, products,
  diagnostics, and runtime concerns

Phase 2 is complete. Iceberg is the active analytical storage layer; legacy Parquet
outputs may still exist under `data/`, but the pipeline no longer updates them.

## Phase 3 - Environmental Insights And Alerts

Already delivered:

- read-only location freshness and latest-value reporting
- finalized hourly trend statistics for curated locations
- explicit `fresh`, `stale`, and `missing` data-availability states

Next outcomes:

1. Ingest weather observations through a dedicated source contract.
2. Store weather data with replay-safe identities and auditable lineage.
3. Join air-quality and weather observations by location and event time.
4. Add city comparison and longer-term time-series views.
5. Introduce explainable anomaly and missing-data alerts.

Phase 3 is complete when the platform provides a reproducible environmental view
that combines air quality and weather, supports comparison over time, and emits
actionable alerts with traceable evidence.

## Phase 4 - Operations And Cloud

Planned outcomes:

- metrics and structured logs for ingestion, processing, freshness, and failures
- Kafka lag and checkpoint health monitoring
- infrastructure defined with Terraform
- reproducible cloud deployment and documented operating procedures
- CI/CD and security checks appropriate for the deployed environment

## Success Criteria

| Dimension | Evidence |
|---|---|
| Product | Multiple real locations, understandable trends, and useful alerts |
| Streaming | Restartable event-time processing with explicit late-data behavior |
| Lakehouse | Idempotent tables, auditable lineage, replay, and controlled maintenance |
| Data quality | Explicit contracts, quarantine reasons, and freshness states |
| Operations | Automated checks, monitoring, deployment, and runbooks |
| Portfolio | Clear engineering decisions and a reproducible end-to-end demonstration |

The roadmap is outcome-driven. New tools are added only when they improve one of
these outcomes rather than serving as isolated technology demonstrations.
