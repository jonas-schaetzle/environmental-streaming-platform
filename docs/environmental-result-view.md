# Environmental Result View

The static view in `web/` shows the same selected pollutant across active OpenAQ
stations and their mapped Open-Meteo model locations. Use the
[README commands](../README.md#environmental-result-view) to prepare the reports
and serve only `data/products/view/` on loopback. A current browser is sufficient.

## Read The View

- **Station comparison** shows the latest measurement for the selected pollutant
  at each matching OpenAQ station ID. Freshness describes its age when the
  latest-measurement report was generated. Each card also shows finalized air,
  complete weather and joined hour counts for the displayed hourly window.
- **Hourly trends** share the UTC time axis and use separate vertical scales and
  units for air and weather. Select temperature, precipitation, relative
  humidity, wind speed or sea-level pressure. Missing values break a line.
  Isolated values remain visible as dots. A dry hour can have a model
  precipitation total of zero.
- **Compare a single hour** exposes the plotted values and air observation count
  in a table. It defaults to the most recent hour with any finalized air value,
  or to the final hour in the report when none is available. The dashed marker
  identifies the selected hour on both charts.

Weather is model context for each mapped location. Precipitation is the total
for the displayed hour, with its source timestamp at the end of that hour.
Other plotted weather values are valid at the start. Stations can have different
local surroundings. Use the table to compare the same hour rather than comparing
the latest readings as though they were simultaneous.

## Refresh And Reproduce

Run the two report commands again and reload the browser. The README commands
publish each file only after its report command succeeds. Each report shows its
own generation time. A missing latest-measurement report leaves the hourly
comparison usable.

Pass `--parameter`, `--hours` or `--end-at` to the hourly command to change the
view's pollutant or time range. Supply the same OpenAQ location configuration
to both commands when comparing a custom station selection. Preserve the
underlying [hourly JSON](hourly-environmental-insights.md) with its Iceberg
snapshot IDs to reproduce historical charts. Latest measurements remain a
separate report of the source values at refresh time.

Keep `data/products/view/` limited to the three assets and the two reports. The
future single-machine deployment can serve that directory on loopback and
forward the port over SSH.

## Local Verification

On 2026-11-06 the view was checked in a browser with a real 48-hour PM2.5
report ending 2026-11-04 at 14:00 UTC and a newly generated latest-measurement
report. Munich and Stuttgart each showed one finalized hour, Hamburg showed
none, and all three had six hours of complete weather. The shared-hour selector
showed zero precipitation in an available dry hour and missing precipitation
in a gap. The two charts shared the selected-hour marker, displayed the two
isolated air values and broke each weather series across the collection gap.
