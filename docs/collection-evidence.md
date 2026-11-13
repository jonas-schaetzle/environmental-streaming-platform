# Local Collection Evidence

## Reconcile One Run

The 2026-11-11 collection run polled all three configured locations twice per
source. The first cycle published 15 OpenAQ and 126 Open-Meteo events. The
second cycle published zero events. The weather producer identified all 126
repeated observations as unchanged.

| Source | Published by producers | Kafka records covered by committed Spark checkpoints | Matching canonical rows | Matching quarantine rows |
|---|---:|---:|---:|---:|
| OpenAQ | 15 | 15 | 15 | 0 |
| Open-Meteo | 126 | 126 | 126 | 0 |

The comparison uses the half-open Kafka offset ranges below. The canonical and
quarantine checkpoints covered the same range for each source. The air-quality
hourly checkpoint covered the OpenAQ range as well. Every topic, partition and
offset in these ranges appears once across the corresponding canonical and
quarantine snapshots. Each Spark query reads its source independently.

| Topic | Partition 0 | Partition 1 | Partition 2 |
|---|---|---|---|
| `environment.measurements.canonical` | [13, 17) | [21, 28) | [17, 21) |
| `environment.weather.canonical` | [408, 534) | [0, 0) | [0, 0) |

The canonical row totals rose from 51 to 66 for air quality and from 144 to
270 for weather. The air-quality quarantine stayed at zero. The six weather
quarantine rows predate this run. The hourly table gained 15 aggregate rows
for **2026-11-04** after the newer input advanced the watermark. These are
aggregated station, pollutant and hour combinations from earlier observations.
See the [event-time verification](event-time-verification.md) for a replay
with controlled arrival order.

The producer reports and Iceberg audit are under ignored
`data/collection/20261111T132628Z_htb5j6hl/`. The reconciliation uses the previous
and current audit snapshots and Spark checkpoint offsets with matching commit
files. The archived evidence for this check is under ignored
`data/verification/collection_evidence_20261113/`.

## Compare Station And Parameter Coverage

The coverage scan used the same post-run Iceberg snapshots and examined 216
completed UTC hours per station from 2026-11-03 00:00 through 2026-11-12 00:00
with the end excluded. Each air-quality cell gives the count of distinct
canonical observation hours, finalized hours, and finalized hours paired with
all six weather values. Subtract each count from 216 for the corresponding
number of missing hours. The counts describe the collected data.

| Pollutant | Munich Stachus (2669) | Stuttgart Bad Cannstatt (2936) | Hamburg Sternschanze (3015) |
|---|---|---|---|
| PM2.5 | 3 / 2 / 2 | 3 / 2 / 2 | 2 / 1 / 1 |
| PM10 | 3 / 2 / 2 | 3 / 2 / 2 | 2 / 1 / 1 |
| NO₂ | 3 / 2 / 2 | 3 / 2 / 1 | 2 / 1 / 1 |
| O₃ | 3 / 2 / 2 | 3 / 2 / 2 | 2 / 1 / 1 |

The canonical data also contained CO in three Munich hours and two Hamburg
hours, and SO₂ in two Hamburg hours. The comparison view supports the four
pollutants in the table. The 2026-11-11 canonical observations did not yet have
finalized hours in the pinned hourly snapshot.

| Weather parameter | Munich | Stuttgart | Hamburg |
|---|---:|---:|---:|
| Temperature | 15 / 216 | 15 / 216 | 15 / 216 |
| Relative humidity | 15 / 216 | 15 / 216 | 15 / 216 |
| Precipitation | 15 / 216 | 15 / 216 | 15 / 216 |
| Wind speed | 15 / 216 | 15 / 216 | 15 / 216 |
| Wind direction | 15 / 216 | 15 / 216 | 15 / 216 |
| Sea-level pressure | 15 / 216 | 15 / 216 | 15 / 216 |

Each weather value follows the report's UTC alignment. Precipitation belongs to
the preceding hour and a recorded zero counts as present. See
[Hourly Environmental Insights](hourly-environmental-insights.md#time-meaning-and-missing-data).

## Reproduce The Selected Demo Window

The scan compared 211 rolling six-hour windows for each of the four pollutants.
It ranked windows by the number of stations with a finalized air-quality hour
paired with complete weather, then by simultaneous comparison hours at two or
more stations, total paired station-hours, and complete weather station-hours.
Ties favored PM2.5 and then the more recent window.

The selected PM2.5 report covers **2026-11-04 10:00–16:00 UTC**. Select
**12:00 UTC** in the view to compare all three stations for the same hour.
Each station has three complete weather hours and one paired PM2.5 hour.
The window demonstrates a station comparison and contains explicit gaps.

| Station at 12:00 UTC | PM2.5 mean (µg/m³) | PM2.5 observations | Model temperature (°C) |
|---|---:|---:|---:|
| Munich Stachus | 13.45 | 1 | 5.6 |
| Stuttgart Bad Cannstatt | 11.90 | 1 | 7.8 |
| Hamburg Sternschanze | 3.643 | 1 | 8.9 |

```bash
mkdir -p data/products/demo-view
cp web/index.html web/view.css web/view.js data/products/demo-view/
python -m environmental_streaming.products.hourly_environmental_insights \
  --parameter pm25 --hours 6 --end-at 2026-11-04T16:00:00Z \
  --air-snapshot-id 5234704772541899633 \
  --weather-snapshot-id 6808911658487011702 \
  > data/products/demo-hourly.tmp &&
mv data/products/demo-hourly.tmp data/products/demo-view/hourly_environmental_insights.json
python -m http.server 8765 --bind 127.0.0.1 --directory data/products/demo-view
```

Open [the local view](http://127.0.0.1:8765/) to inspect the historical report.
The demo directory contains the hourly comparison. The station cards use a
separate latest-measurement report when available. See
[Environmental Result View](environmental-result-view.md).

Regeneration requires the retained snapshots in the local warehouse and the
same station mappings. The archived report remains available independently
of snapshot retention. The command reproduced all report fields except the
generation timestamp on 2026-11-13.
