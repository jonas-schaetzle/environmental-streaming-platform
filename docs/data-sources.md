# Curated OpenAQ Source Coverage

## Active Stations

`config/openaq_locations.json` defines the stations polled by default and shown in
current air-quality insights. It is an operational selection, not a guarantee that
every sensor is current or every hour is complete.

| City | OpenAQ ID | Active station |
|---|---:|---|
| Munich | 2669 | Munich Stachus |
| Stuttgart | 2936 | Stuttgart Bad Cannstatt |
| Hamburg | 3015 | Hamburg Sternschanze |

The configured labels are display names. OpenAQ location and sensor IDs identify
observations; a display label or city must not replace those identities.

## Hamburg Selection On 2026-11-04

A live check confirmed that Hamburg Hafen (3071) still had a newest source time of
2026-09-21T08:00:00Z. Munich and Stuttgart had readings dated 2026-11-04T12:00:00Z.
The old Hamburg source was reachable, so request-success coverage alone did not
reveal its stale measurements.

An OpenAQ search within 25 km of the unchanged Hamburg weather reference point
returned 39 locations on a complete result page. The comparison selected stationary
EEA reference monitors with common air-quality parameters, rather than mixing in
consumer-device measurements. Latest values were checked per sensor, not only via
the location's maximum `datetimeLast`.

| Candidate ID | Station | Parameters with current readings at 12:00Z on Nov 4 |
|---|---|---|
| 3010 | Hamburg Habichtstraße | CO, NO2, PM10, PM2.5 |
| 3014 | Hamburg Wilhelmsburg | O3, PM10, SO2, NO2, PM2.5 |
| 3015 | Hamburg Sternschanze | SO2, CO, NO2, O3, PM10, PM2.5 |
| 3095 | Hamburg Veddel | PM10, SO2, NO2, PM2.5 |

Sternschanze provides the broadest current parameter coverage among these checked
candidates. The [Hamburger Luftmessnetz station description](https://luft.hamburg.de/allgemeine-informationen/13st-sternschanze-774872)
classifies it as an urban background/ozone station. It is a selected station, not
a citywide average; comparisons must show station labels and must not imply that
all locations measure equivalent traffic/background exposure.

All four candidates also returned an old NO sensor value dated 2024-03-11.
Sternschanze's current six parameters do not make that NO reading fresh. Keep the
source timestamp and expose per-parameter staleness; do not rewrite times, replace
missing values with zero, or discard old valid observations as malformed.

The read-only search and candidate snapshots are saved locally under ignored
`data/verification/hamburg_coverage_20261104/`. They are dated evidence, not a promise
of future source availability. Recheck actual sensor freshness during operation.

## Active Selection And Historical Mapping

Starting with this configuration update, default OpenAQ polling replaces Hafen
3071 with Sternschanze 3015. Current insights select 3015 explicitly and do not use
old Hafen rows as a fallback for the new station.
Retirement here describes the project's active selection, not a claim that the
operator has closed the physical station. Future gaps should trigger review and
freshness alerts rather than automatic station reassignment.

`config/weather_locations.json` retains **both 3071 and 3015** under the fixed
`hamburg` weather reference. Its ID and requested coordinates do not change.
Every active station must map exactly once; retired stations may remain mapped
when needed to interpret historical rows. A configuration test asserts active
coverage and the explicit retired set `{3071}`.

This is an additive historical mapping policy: both station IDs refer to the same
city-level weather context, while their air-quality observations remain distinct.
Joining a historical row uses its original OpenAQ ID and actual UTC time. The
mapping does not assert that weather was measured at either station or that the
two air-quality series are interchangeable. Keep station IDs in grouped outputs;
do not splice Hafen history into a Sternschanze trend or interpolate the gap.
Only join weather actually available for the requested hours.

Existing 3071 Kafka/lake rows retain their IDs, coordinates, timestamps and Kafka
lineage. Existing OpenAQ producer-state entries for 3071 are retained; new 3015
sensor identities acquire their own progress after confirmed delivery. Do not
clear state to activate the new station. The same Kafka topic, Spark checkpoints,
Iceberg identities and schemas continue unchanged; no lake migration is needed.
A first fetch for 3015 can include old-but-valid sensor values as well as current
ones, which remains visible in per-parameter freshness.

Weather fingerprints, event contracts, city reference coordinates and dataset
identity are unchanged. Extending the station mapping does not create a new weather
revision or justify resetting weather producer state. Saved captures/replays keep
their original receipt timestamps and payloads.

## Collection Limits

OpenAQ's [Latest resource](https://docs.openaq.org/resources/latest) exposes the last
value in each sensor's time series, rather than a complete change feed. Upstream
measurements can arrive out of order; polling latest values cannot guarantee a
complete historical hourly series. Data collected locally is limited by the
actual polling schedule, API availability and Kafka retention. Describe hourly
statistics as statistics of ingested observations, not official provider hourly
means or proof that every hour was sampled.

Longer gaps need an explicit catch-up/backfill policy within source availability.
Replacing a stale station improves current coverage but does not backfill its
missing history. Freshness thresholds and missing-hour alerts remain product
requirements; source HTTP success and location-level maxima are insufficient.

## Local Verification On 2026-11-04

| Check | Evidence |
|---|---|
| OpenAQ real-source ingestion | All three active locations succeeded; 16 new events published |
| Repeated ingestion with persisted progress | Second cycle published zero unchanged events |
| Sternschanze storage | Seven canonical events, six fresh parameters and one stale NO parameter |
| Historical Hafen data | All five prior canonical rows retained unchanged, including Kafka lineage |
| Historical Hafen producer state | All five retired sensor-state entries retained unchanged |
| End-to-end path | Both live sources reached Kafka and the existing Spark/Iceberg sinks |
| Audit | All five tables healthy with zero duplicate or incomplete identities |
| Quality checks | Ruff and all 101 tests passed |

The resulting tables contain 51 canonical measurements, zero measurement quarantine
rows, 35 finalized hourly aggregates, 144 canonical weather rows and the same six
pre-existing weather quarantine rows. This bounded check does not establish
continuous collection, complete hourly sampling, automatic source failover, or
polling-gap recovery. No topic, checkpoint, producer state or lake table was reset.
