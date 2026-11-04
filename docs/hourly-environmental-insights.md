# Hourly Air-Quality And Weather Insights

`products/hourly_environmental_insights.py` reads finalized OpenAQ hourly
aggregates and canonical Open-Meteo weather from Iceberg. It emits a version-1 JSON
report with one series per active monitoring station and a complete UTC hour grid.
It is a bounded product refresh; it does not create tables, modify source data,
advance checkpoints or fetch APIs. Kafka does not need to be running.

## Run

Run the measurement stream and both weather sinks first so the input tables exist.
Then generate the most recent 48 completed UTC hours for PM2.5:

```bash
python -m environmental_streaming.products.hourly_environmental_insights
```

Select a parameter and an explicit end time for a bounded historical comparison:

```bash
python -m environmental_streaming.products.hourly_environmental_insights \
  --parameter no2 --hours 24 --end-at 2026-11-04T14:00:00Z
```

Parameters are `pm25`, `pm10`, `no2` and `o3`, all selected in canonical `µg/m³`.
Different parameters, sources and units are not averaged together. A mismatched
unit has no selected aggregate. The range is 1-744 hours, default 48, ending at
`--end-at` exclusively. The end must be an explicitly offset, completed hour;
aware offsets are converted to UTC. Without it, the current UTC hour is used as
the exclusive end. Future/partial hours and naive timestamps are rejected.

`--locations-path` and `--weather-locations-path` select alternative validated
configurations. Each active OpenAQ station needs exactly one weather mapping.
Historical mapped stations are not included unless explicitly selected as active.
Keep outputs under ignored `data/`, for example:

```bash
mkdir -p data/products
python -m environmental_streaming.products.hourly_environmental_insights \
  > data/products/hourly_environmental_insights.json
```

This replaces that report file only; retain separately named artifacts when
comparing refreshes.

## Time Meaning And Missing Data

| Output | Relation to air-quality window 10:00-11:00 UTC |
|---|---|
| Air-quality count/mean/minimum/maximum | Existing finalized statistics of ingested observations in the window |
| Temperature, humidity, wind speed/direction, sea-level pressure | Canonical point value valid at 10:00, not an hourly mean |
| Precipitation | Sum labelled 11:00, describing the preceding 10:00-11:00 hour |

The time meaning follows the existing weather contract and
[Open-Meteo's parameter documentation](https://open-meteo.com/en/docs). Every weather
value retains its unit, valid time, receipt time, requested/grid coordinates and
accepted Kafka topic/partition/offset. Attribution is included in the report.
Weather provides model-based city context rather than measurements at each station.

Each station has exactly the requested number of hour entries, including gaps.
Air quality is either `available` or `missing_finalized_aggregate`; the latter has
`air_quality: null`. It means no finalized selected aggregate is available, which
can reflect missing observations or the streaming watermark still waiting for later
event-time input. It does not prove the source never observed that hour. Current
measurements can be visible in the separate air-quality insights report while
hourly statistics remain unfinalized.

Weather is `complete` when all six contract parameters are present, `partial` when
some are present, or `missing` when none are present. Missing parameters are null;
a real precipitation value of zero remains zero. Weather can remain available when
air quality is absent, and air-quality statistics remain available without weather.
No interpolation, zero filling, hourly averaging of weather point values or
cross-station fallback is performed.

Coverage counts report expected hours, selected finalized air-quality hours,
complete weather hours and hours containing both. They are availability counts,
not assertions of full observation sampling. Station names/IDs remain in every
series. Comparing selected stations does not establish a citywide exposure ranking;
background and traffic contexts can differ. Hafen history is not spliced into
Sternschanze; see the [station-transition policy](data-sources.md).

## Snapshots, Corrections And Reproduction

Each input is pinned to its current Iceberg `main` reference at the start of the
read. The report includes both table names and snapshot IDs under `inputs`, as
well as its parameter, unit, UTC range, active station labels and weather mappings.
Pinning is per table; there is no cross-table transaction or claim of a single
atomic source cutoff. The reader uses Spark 4.1's `versionAsOf` option for the
pinned Iceberg version.

A refresh normally uses the newest accepted canonical weather snapshot. If a newer
response corrects an old hour, regenerating the same range uses the accepted new
value and receipt/Kafka lineage, while the finalized air statistics remain separate.
Receipt time does not become event time and is not constrained to the historical
hour being reported. The report is retrospective context, not a forecast issued at
that historical hour. The existing canonical writer owns revision ordering and
conflict handling; the product does not implement another revision history.

To reproduce an earlier report, reuse its parameter, hours, end time, active
station/mapping configuration and set `--air-snapshot-id` and
`--weather-snapshot-id` to its recorded `inputs` IDs. Results are identical apart
from `generated_at_utc` while those snapshots and compatible schemas remain
available. Snapshot expiration can remove that ability. Unknown/expired versions
fail explicitly rather than falling back to latest. An empty table with no
snapshot produces an all-missing input and a null snapshot ID; there is no past
snapshot to replay for that empty state.

Missing tables fail; the report never creates them. Duplicate selected aggregate
keys or duplicate canonical weather parameter/hour keys fail rather than inflating
the station series or choosing arbitrary values. Missing/ambiguous configuration
mappings fail before table reads. All report timestamps use explicit UTC and avoid
host-local datetime conversion, including nested weather receipt times.

## Verification

Focused tests cover precipitation alignment and true zero values, point-weather
labels, complete/partial/missing context, unit/source/dataset filtering, retained
station identities, refreshed corrections, duplicate inputs and bounded UTC ranges.

On 2026-11-04, the real 48-hour PM2.5 report for
2026-11-02T14:00:00Z-2026-11-04T14:00:00Z contained:

| Active station | Expected hours | Finalized air hours | Complete weather hours | Complete joined hours |
|---|---:|---:|---:|---:|
| Munich Stachus | 48 | 1 | 6 | 1 |
| Stuttgart Bad Cannstatt | 48 | 1 | 6 | 1 |
| Hamburg Sternschanze | 48 | 0 | 6 | 0 |

The sparse history reflects bounded local collection. In particular, Sternschanze
had current canonical observations but no finalized selected hourly aggregate yet.
The separate latest-value report and this finalized-hour report answer different
questions. More continuous local collection is needed for useful longer trends;
this implementation does not manufacture them from the existing small sample.

The real report was reproduced from both recorded snapshot IDs under a different
Python host timezone, matching apart from its generation time. An isolated local
Iceberg catalog, seeded with copied real input rows, additionally verified empty
tables, a simulated newer weather correction, reproduction from old snapshots,
non-reversion by older replay, preserved lineage on identical replay, and explicit
failure for an unknown snapshot. Only that isolated test copy was modified.
All 438 captured production warehouse/state/checkpoint files remained unchanged.
Ruff and all 107 tests passed. Evidence and the generated report remain under
ignored `data/verification/hourly_environmental_20261104/`.

This package adds the report contract and command, not a graphical view or alert
notifications. Real Kafka/API ingestion was not rerun because no ingestion or
streaming behavior changed; the preceding source package verified that path.
Host reboot, Linux/AWS deployment, complete collection/catch-up and the remaining
cloud/budget checks remain outside this report verification.
