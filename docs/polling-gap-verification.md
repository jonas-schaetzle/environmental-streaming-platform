# Polling Gap Verification

The bounded weather request can recover hours missed during a polling gap while
those hours remain within the requested source range. The local collector uses
a six-hour overlap. To verify that behavior, run the existing producer and Spark
sinks with a dedicated broker, producer state, warehouse and checkpoints.

## Run

Start a fresh [isolated Kafka broker](event-time-verification.md#run-locally) on
port 19092. The broker must have no `environment.weather.canonical` topic.

```bash
python scripts/verify_polling_gap.py
```

The script requests live weather for the three configured reference locations.
It stages missed polls by publishing only the oldest hour of each initial
response. It then calls the producer again with the full six-hour request and
the same saved state. Both publications use Kafka and the existing Spark sinks.
The resumed requests call the real Open-Meteo API. The elapsed outage is simulated.

The assertions check that every returned parameter/hour reaches its canonical
business key, values and units match the resumed source response, previously
absent hours appear, no duplicate keys exist, and quarantine remains empty.
The report and both raw response sets are retained under ignored
`data/verification/polling_gap_*/`. Stop the isolated broker after the run.

## Source And Processing Limits

Weather catch-up covers the returned source range. Check source availability and
select an explicit overlap for the gap you intend to recover. Hours outside the
range remain absent. Recovered weather values reflect the model response at the
later fetch time and retain that fetch timestamp.

The OpenAQ producer calls `/locations/{id}/latest` and deduplicates each sensor by
its latest published timestamp. A later poll can deliver a newer reading, while
intermediate observations require a separate historical sensor query. The
existing latest-reading poll does not recover arbitrary OpenAQ history.
The [collection coverage](collection-evidence.md) records the actual collected
station hours.

Spark resumes retained Kafka records through its independent checkpoints. Source
hours never published to Kafka require source retrieval. The
[event-time check](event-time-verification.md) shows that a late air-quality
record can enter canonical storage after its aggregate window has closed.

## Verified Result

On 2026-11-21, each resumed Open-Meteo response returned seven hours for six
parameters. The staged initial state held one hour per location. Reusing that
state published 108 missing parameter/hour observations, skipped 18 unchanged
ones and grew the isolated canonical table from 18 to 126 rows. All three
locations recovered 04:00–09:00 UTC on November 21. Quarantine remained empty.
Raw source responses and the report are retained under ignored
`data/verification/polling_gap_gi43l21e/`.

A read-only check of the three OpenAQ latest endpoints on the same day found
current readings at 08:00 UTC alongside older sensor timestamps. Munich returned
seven records, Stuttgart six and Hamburg seven. This confirms the need to assess
freshness per sensor. The check retained only sensor IDs and timestamps under
ignored `data/verification/openaq_gap_review_20261121.json`.
