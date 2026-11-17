# Weather Correction Verification

Replaying an archived Open-Meteo observation followed by a controlled correction
tests the existing canonical writer through Kafka, Spark Structured Streaming and
Iceberg. Both records describe the same hour and have the same business key
(source, dataset, weather location, parameter and validity time). The second
record changes the temperature by 2 °C and its fetch time by two minutes. These
changes simulate a correction.

The script then replays the correction and the archived version, in that order.
It starts an availableNow query after each publication. All four runs use the
same checkpoint and each consumes one input record at a new Kafka offset.
Iceberg keeps one canonical row throughout. The correction creates a new snapshot
and updates the row's value, fetch time and Kafka identity. Both replays leave
those accepted fields in place.

## Run Locally

Use the [isolated broker setup](event-time-verification.md#run-locally) on port
19092. Prepare a canonical weather JSONL file with a non-null temperature using
the [capture and export commands](../README.md#weather-source-check), then pass its
path to the script from the repository root.

```bash
python scripts/verify_weather_correction.py "$canonical_weather_file"
```

The script creates a new topic and writes its own warehouse, checkpoint and
`report.json` under ignored `data/verification/`. Stop the isolated broker after
the run. The report records the snapshot, query ID, run ID, consumed offset and
stored Kafka identity after each step.

## Verified Result

The 2026-11-16 run used an archived Munich temperature valid at 12:00 UTC on
2026-10-06. The fetch times in the table are also from 2026-10-06, in UTC.

| Stage | Temperature | Fetch time | Kafka partition and offset | Canonical rows |
|---|---:|---|---|---:|
| Archived record | 20.5 °C | 15:07:42.202170 | 0 / 0 | 1 |
| Simulated correction | 22.5 °C | 15:09:42.202170 | 0 / 1 | 1 |

The second run resumed the same query checkpoint, committed a new Iceberg
snapshot and preserved the original business key. The canonical row now carries
the correction's fetch time and Kafka identity.

The 2026-11-17 run repeated the correction and checked both replay orders.
Spark consumed one record in each run from the same checkpoint on partition 0.

| Step | Consumed Kafka offset | Stored Kafka offset | Stored temperature | Canonical rows |
|---|---:|---:|---:|---:|
| Archived record | 0 | 0 | 20.5 °C | 1 |
| Simulated correction | 1 | 1 | 22.5 °C | 1 |
| Identical replay of the correction | 2 | 1 | 22.5 °C | 1 |
| Replay of the older record | 3 | 1 | 22.5 °C | 1 |

Iceberg committed an overwrite snapshot for each replay in this run. Each
snapshot replaced one physical row and retained the same canonical row and
Kafka identity.
