# Technical Interview Walkthrough

## Explain A Late Measurement

**Situation.** OpenAQ observations can arrive out of order. A station's latest
reading and its finalized hourly mean can therefore describe different input
sets.

**Decision.** Canonical storage retains valid records with their Kafka identity.
Hourly aggregation uses measurement time, one-hour windows and a two-hour
watermark. A read-only batch recalculation can quantify the impact of a later
arrival.

**Evidence.** The controlled local run finalized two observations at
16.725 µg/m³. A third record arrived after closure and was retained in canonical
Iceberg. Recalculating the hour from three records gave 24.483 µg/m³.
Both queries resumed their checkpoints with zero new input and stable snapshots.
The check also exposed empty aggregate MERGE commits, which the hourly sink now
skips.

**Discussion.** Explain why event-time completeness depends on incoming data,
how an open window differs from a finalized one, and what evidence a reviewed
historical correction would need.

[Reproduce the event-time check](event-time-verification.md).

## Explain A Correction And Replay

**Situation.** A new Open-Meteo response can correct an earlier hour. Kafka also
assigns new offsets when the same payload is published again.

**Decision.** Weather uses a business key comprising source, dataset, location,
parameter and validity time. Fetch time orders accepted revisions. The accepted
row retains its Kafka identity.

**Evidence.** A controlled correction replaced 20.5 °C with 22.5 °C for the same
hour and kept one row. Identical and older replays arrived at offsets 2 and 3,
while the canonical value and accepted offset 1 persisted.

**Discussion.** Explain the difference between transport identity and business
identity. The replay check observed physical Iceberg overwrite snapshots even
though the logical row stayed the same. Snapshot IDs alone cannot establish
a changed value.

[Reproduce the correction and replay check](weather-correction-verification.md).

## Diagnose A Data-Quality Alert

**Situation.** A weather payload had its value deliberately replaced with null.

**Decision.** Validation routes it into a separate quarantine table with raw
payload, validation reason and Kafka metadata. A bounded quality report exposes
that evidence.

**Evidence.** The alert reported `missing_value` at partition 0, offset 0.
The diagnostic reader retrieved that exact Kafka record. Its bytes, key and
timestamp matched the quarantined Iceberg row. The canonical table had zero
rows.

**Discussion.** Explain how the operator goes from a symptom to the original
record, why invalid source timestamps should not determine quarantine partitions,
and how a replay at a new Kafka offset can increase transport-row counts.

[Reproduce the alert trace](data-quality-alerts.md#trace-a-weather-alert-to-kafka).

## Demonstration Context

The checks use real archived source observations with controlled modifications,
and isolated local runtime state. Use the
[measured coverage and selected comparison window](collection-evidence.md)
to explain the current limits of the station history.
