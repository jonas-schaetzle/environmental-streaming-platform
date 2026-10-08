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

## Planned Weather Ingestion

The weather event model, location configuration, Open-Meteo HTTP client, and
canonical event conversion are implemented and tested. The client supports
one-shot raw captures, which can be exported as canonical JSON Lines events and
explicitly replayed to Kafka. Producer polling and weather storage remain implementation
targets for Phase 3. They will use separate source and stream modules within the
existing packages, with table definitions owned by `lakehouse`.

### Source And Coverage

Use [Open-Meteo's Forecast API](https://open-meteo.com/en/docs) at
`https://api.open-meteo.com/v1/forecast` for hourly model-based weather values at
configured coordinates. These values provide environmental context for OpenAQ
stations; they are not weather-station measurements. The initial dataset is
`forecast_best_match`, which records the automatic model-selection policy rather
than claiming that a single underlying model was used.

`config/weather_locations.json` defines weather locations for Munich, Stuttgart,
and Hamburg. `ingestion/weather_locations.py` loads and validates the configuration
as immutable `WeatherLocation` objects. Each
location has a stable, project-owned `weather_location_id`, a city label, requested
coordinates, and an explicit mapping from OpenAQ location IDs. Several OpenAQ
stations may share one weather location. A city-level weather location represents
regional context, not conditions measured at each station. Changing its requested
coordinates requires a new ID so that historical rows retain their spatial meaning.
Preserve the returned grid coordinates separately from the requested coordinates.

The configured city reference coordinates were checked against Open-Meteo's
Geocoding API on 2026-10-05:
[Munich](https://geocoding-api.open-meteo.com/v1/search?name=Munich&count=1&language=en&format=json),
[Stuttgart](https://geocoding-api.open-meteo.com/v1/search?name=Stuttgart&count=1&language=en&format=json),
and [Hamburg](https://geocoding-api.open-meteo.com/v1/search?name=Hamburg&count=1&language=en&format=json).
They are fixed configuration values, not a geocoding lookup on each run.
`openaq_location_ids` must be a non-empty list of positive integer IDs. Each ID
may appear only once across the entire configuration, while one weather location
may cover multiple stations. The loader rejects duplicate weather IDs, empty
labels, non-numeric or non-finite coordinates, and coordinates outside WGS84
latitude/longitude bounds. It trims surrounding whitespace from weather IDs and
city labels before checking uniqueness. It validates mapping structure; it does
not call OpenAQ to verify station existence or require every configured station
to have weather coverage. A repository test checks that the curated files agree.

Weather IDs identify fixed reference coordinates over time. Editing a city's
reference point requires a new weather ID and an explicit mapping update, even
though no weather processing state exists yet. Station mappings are static for
now; changes after ingestion starts require a documented historical join policy.

Request hourly data in UTC with explicit Celsius, metres-per-second, and millimetre
settings. Initially publish only hours at or before the current UTC hour. Future
forecast hours require a separate product decision. Longer historical backfills
using the [Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api)
must carry a separate dataset identity: reanalysis and recent forecast-model data
must not silently replace each other.

The initial portfolio deployment uses the non-commercial API. Keep Open-Meteo
attribution with weather products and respect the applicable
[API usage terms and data licence](https://open-meteo.com/en/pricing).

### HTTP Client And Raw Captures

`ingestion/weather_client.py` owns HTTP access to the Forecast API. Its
`fetch_hourly_weather` function accepts a validated `WeatherLocation` and returns
an `HourlyWeatherResponse` containing the decoded API payload and one timezone-aware
UTC response-receipt timestamp, assigned before JSON decoding and validation.

The request uses the six contract parameters, UTC, ISO 8601 time labels, Celsius,
metres-per-second wind speed, and millimetre precipitation. Default request bounds
are `past_hours=3` and `forecast_hours=1`, covering the recent overlap and current
hour. Automatic model selection supplies the `forecast_best_match` dataset.
The client verifies a zero UTC offset, a non-empty hourly time array, parameter
arrays of matching length, and the expected source units. Raw ISO time labels have
no offset suffix; `ingestion/weather_events.py` explicitly encodes their UTC meaning
as canonical timestamps. It maps source temperature and wind-direction unit
symbols to `degC` and `degree` before event validation.

The HTTP defaults are a 30-second request timeout, at most four attempts, and
exponential retry delays starting at one second. Connection errors, timeouts, and
HTTP 429/500/502/503/504 responses are retried. Other HTTP failures, invalid JSON,
and malformed response envelopes fail without retry. Missing individual values
remain present as nulls; domain validation belongs to the event model.

The one-shot CLI selects a configured weather ID and creates an exclusive,
timestamped snapshot under `data/weather/`. Captures include requested location
metadata, source/dataset identity, the receipt timestamp, and the response with
its returned grid coordinates. They preserve source evidence without advancing
producer progress or Spark offsets. Snapshots are not canonical events and must
be converted before Kafka replay. Automated publishing, overlap deduplication, and
explicit date-range catch-up remain producer work.

### Canonical Event Conversion

`build_canonical_weather_events` in `ingestion/weather_events.py` converts an
`HourlyWeatherResponse` and a `WeatherLocation` into six event payloads per retained
hour. It revalidates the response envelope, accepts source time labels only in
`YYYY-MM-DDTHH:00` format, rejects invalid dates and duplicate hours, and requires
an explicitly UTC receipt timestamp. Hours after receipt time are excluded, not
compared with the current wall clock. Requested coordinates come from location
metadata; returned grid coordinates come from the source payload.

The converter does not replace missing values or repair domain-invalid values.
They remain available to `processing/weather_model.py` for validation and future
quarantine routing. Missing grid coordinates likewise remain null. Conversion
does not certify that an event is valid.

The export CLI reads a raw capture with the supported source/dataset identity and
uses its saved location metadata and receipt timestamp. It creates an exclusive
`<capture-name>_canonical.jsonl` file under `data/weather/`. Repeating conversion
with the same input yields identical events, even after configuration changes or
at a later date. It does not fetch new data, publish to Kafka, deduplicate repeated
responses, or advance any processing state. The weather contract and all existing
OpenAQ tables and checkpoints remain unchanged; no migration is required.

### Weather Event Contract

Publish one event per location, dataset, parameter, and UTC hour. Weather has its
own contract because model data has no OpenAQ sensor identity.

| Field | Meaning |
|---|---|
| `source` | `open_meteo` |
| `source_dataset` | `forecast_best_match` for the initial source |
| `weather_location_id` | Stable configured weather location ID, such as `munich` |
| `city` | Display label; not a join key |
| `latitude`, `longitude` | Requested WGS84 coordinates from location configuration |
| `grid_latitude`, `grid_longitude` | Grid coordinates returned by the API |
| `parameter` | Parameter from the allowlist below |
| `value` | Finite numeric value |
| `unit` | Canonical unit for the parameter |
| `measured_at_utc` | Source's hourly valid-time label, encoded with an explicit UTC offset |
| `fetched_at_utc` | UTC response-receipt time, assigned once per successful HTTP response |

All listed fields are required. Despite its shared name, `measured_at_utc` is a
model valid time, not evidence of a physical measurement. `fetched_at_utc` orders
the responses received by this platform; it is not a model issue time. An actual
model identifier or run time may only be added when the source provides it.

The implemented model accepts `open_meteo` and `forecast_best_match`. Supporting a
historical dataset requires an explicit extension of validation. Timestamp strings
use ISO 8601 with seconds, an optional fraction of up to six digits, and `Z` or
`+00:00`. Missing offsets and local-time offsets are rejected before routing.
`measured_at_utc` must lie on a full UTC hour and must not exceed `fetched_at_utc`.
This rule excludes future hours without comparing replayed data to the wall clock.

| Parameter | Canonical unit | Time meaning |
|---|---|---|
| `temperature_2m` | `degC` | Value at the labelled hour |
| `relative_humidity_2m` | `%` | Value at the labelled hour |
| `precipitation` | `mm` | Total over the preceding hour |
| `wind_speed_10m` | `m/s` | Value at the labelled hour |
| `wind_direction_10m` | `degree` | Value at the labelled hour |
| `pressure_msl` | `hPa` | Value at the labelled hour |

Validation checks required identities, timestamps, coordinate ranges, finite
values, and parameter/unit compatibility. Humidity is between 0 and 100, wind
direction between 0 and 360, precipitation and wind speed are non-negative, and
pressure is positive. Negative Celsius temperatures are valid. Missing source
values remain missing; they must never become zero. Malformed Kafka events enter
weather quarantine with a machine-readable reason and their transport metadata.

`parse_weather_observations` consumes a DataFrame containing JSON in `raw_value`
and preserves its metadata columns. It produces typed canonical fields and a
comma-separated `validation_error`, which is empty for valid rows. The valid and
invalid filter functions form complementary routes; the valid route drops the
error column. Malformed JSON and type errors are marked `invalid_payload`, and
missing, invalid, or unsupported fields receive specific reasons. Raw payloads
remain available even when parsing fails. Units must match the canonical units in
the table; the model does not infer units or convert incompatible quantities.

This model does not yet write quarantine records or connect to Kafka or Iceberg.
It introduces a separate contract without changing the OpenAQ payload schema,
existing checkpoints, or table identities.

### Kafka And Iceberg Identities

The implemented one-shot replay uses `environment.weather.canonical` with the Kafka key
`open_meteo:<weather_location_id>`. This groups a location's events in one partition;
it does not deduplicate them. Preserve topic, partition, offset, Kafka timestamp,
key, and raw payload in both weather sinks.

`messaging/kafka_publisher.py` owns both source-specific key functions and shared
serialization and delivery checks. The OpenAQ default topic and sensor key remain
unchanged. Delivery callbacks surface permanent failures even when the producer
queue is empty; a 30-second flush timeout bounds the final delivery wait for both
sources. A failed or timed-out batch may already have delivered some records.

`ingestion/weather_replay.py` preflights the complete JSON Lines file for basic
envelope identities and the presence of original timestamps, then publishes its
unchanged payloads. It does not certify domain validity, assign new receipt times,
fetch data, or write progress state. Explicit replay may create duplicate business
keys at new Kafka offsets. Deduplication and newest-response handling remain
planned sink responsibilities, not guarantees of a Kafka key. The diagnostic
reader accepts a topic argument and never commits consumer offsets.

No OpenAQ payload, Kafka identity, Iceberg schema, or checkpoint changes are
introduced by this publishing extension; no data migration is required.

| Planned table | Purpose | Merge identity | Hidden partition |
|---|---|---|---|
| `local.lake.canonical_weather_observations` | Latest accepted weather value per business key | Source, dataset, weather location, parameter, and `measured_at_utc` | Day of `measured_at_utc` |
| `local.lake.quarantine_weather_observations` | Invalid weather events and Kafka lineage | Kafka topic, partition, and offset | Day of `kafka_timestamp` |

The canonical business key survives republishing with new Kafka offsets. Insert
new keys and update existing keys only when the incoming `fetched_at_utc` is newer.
An older response or an identical replay cannot overwrite a later accepted row.
Reduce each micro-batch to one newest response per business key before `MERGE`;
conflicting values with equal fetch timestamps must be surfaced as a conflict.
This assumes a single active producer per configured location and a reliable UTC
clock. Multiple writers would require a stronger revision-ordering contract.

Canonical weather rows retain the payload and Kafka lineage of the accepted
response. This is a latest-value table, not a permanent history of every revision.
Older revisions remain available only within Kafka and Iceberg snapshot retention.

### Replay, Progress, And Time

Poll with a configurable overlap, initially the last three hours plus the current
hour, so revised values for an existing hour can be published. Persist a content
fingerprint per business key after confirmed Kafka delivery; exclude
`fetched_at_utc` from the fingerprint so unchanged responses are suppressed.
Republish changed values even when their event time has already been seen.
Keep producer state under `data/state/` and fingerprint retention bounded to the
active polling range. A timestamp-only high-water mark would suppress corrections.

A crash after Kafka delivery but before state persistence can produce a duplicate;
the sink's business key handles it. Replay saved events with their original fetch
timestamps. A new API fetch is a new response, whereas replaying an existing payload
is not. Corrections outside the overlap and gaps after a longer outage require an
explicit date-range catch-up within source availability; the overlap alone does not
guarantee complete history.

Weather canonical and quarantine queries will own separate checkpoints at
`data/checkpoints/weather_canonical` and `data/checkpoints/weather_quarantine`.
They initially follow the existing `availableNow` processing pattern. Existing
checkpoints own continuation; only a new checkpoint starts at earliest offsets.
Business-key merges also protect the canonical sink when republished events have
new transport identities. Producer state, checkpoints, and lake data are never
deleted to trigger replay.

Use `measured_at_utc` for event-time alignment and `fetched_at_utc` for ingestion
lineage. The initial weather sinks have no aggregation watermark and accept old
valid rows and newer responses for old hours. The existing two-hour OpenAQ
watermark remains specific to air-quality aggregation. Later joins must explicitly
define how weather corrections update already-produced analytical results.

Join through the configured location mapping and UTC time, accounting for each
parameter's time meaning. For example, precipitation labelled `11:00Z` describes
the hour from `10:00Z` to `11:00Z`, so it aligns with the air-quality window starting
at `10:00Z`. Temperature labelled `11:00Z` is a point value and must not be presented
as that window's hourly average.

This extension adds topics, tables, and checkpoints without migrating the existing
OpenAQ contract or state. Contract and identity changes after the first weather
deployment require an explicit compatibility and migration decision. A second
stream processor such as Flink remains conditional on a concrete low-latency use
case that justifies another runtime and state model.
