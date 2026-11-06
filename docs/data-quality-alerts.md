# Data-Quality Alerts

Run the bounded JSON report after the availableNow streaming jobs. It reads the
existing Iceberg tables, including both quarantine sinks. Kafka can be stopped.

```bash
mkdir -p data/products
python -m environmental_streaming.products.data_quality_alerts \
  > data/products/data_quality_alerts.tmp && \
  mv data/products/data_quality_alerts.tmp data/products/data_quality_alerts.json
```

A successful report exits with status 0. Read the JSON `status` field, which is
`alerts` if any rule fired and `clear` otherwise. A failed read, such as a missing
input table, fails the command and leaves the previous published file intact.
Check `generated_at_utc` to identify when that file was refreshed. Alerts describe
the state of data collection and processing.

## Rules And Evidence

| Rule | Default trigger | Evidence |
|---|---|---|
| Missing air measurement | No canonical reading for the selected pollutant at the configured station | Station ID, pollutant and freshness threshold |
| Stale air measurement | The latest reading is older than 120 minutes | Measurement time, age and threshold |
| Low finalized air coverage | Fewer than 50% of the last 48 complete UTC hours have a finalized selected aggregate | Station, available and expected hours, range and Iceberg snapshot IDs |
| Low complete weather coverage | Fewer than 50% of the same hours have all weather parameters | Weather mapping, available and expected hours, range and snapshot IDs |
| Recent quarantine | At least one quarantined row with a Kafka timestamp in the last 24 hours | Table, counts by validation error string and latest Kafka topic, partition and offset for each group |

Freshness and hourly coverage are separate signals. A station can have a recent
measurement while its corresponding aggregate is still waiting for the streaming
watermark. Weather hours with only some parameters count as incomplete. Exactly
50% coverage meets the default threshold. Coverage checks reuse the
[joint hourly report](hourly-environmental-insights.md) and its time alignment.

Both time windows include their start and exclude their end. The quarantine window
uses the Kafka record timestamp and excludes rows without that timestamp. Counts
are Kafka transport rows, so replays at new Kafka offsets can contribute additional
rows. A validation error string can contain multiple reasons. The report groups
the full string so each row contributes once to the total. Inspect the
quarantine table or Kafka for the original record when needed.

## Configuration

The default pollutant is `pm25`. Select `pm10`, `no2` or `o3` with
`--parameter`. Use `--coverage-hours` and `--quarantine-hours` for windows
between 1 and 744 hours, `--minimum-coverage-percent` for an integer between
1 and 100, and `--freshness-threshold-minutes` for a positive integer. Use
`--locations-path` and `--weather-locations-path` for matching custom
configurations.

The coverage defaults provide an initial operating threshold. Review them against
the actual local collection schedule before treating frequent alerts as failures.
The report runs on demand and can be retained as evidence under ignored `data/`.

## Local Verification

On 2026-11-06 the default PM2.5 run reported stale measurements and low
finalized-air and complete-weather coverage at all three active stations.
Both quarantine tables had zero rows in the previous 24 hours. With
`--quarantine-hours 744`, the same read found six retained weather quarantine
rows from the October validation check, in two error groups with Kafka lineage.
The verification reports are under ignored `data/verification/data_quality_alerts_20261106/`.
