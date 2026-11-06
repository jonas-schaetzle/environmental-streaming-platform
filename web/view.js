"use strict";

const byId = (id) => document.getElementById(id);
const hourMs = 3_600_000;
const colors = ["#076954", "#a4461b", "#4254a1", "#8d3672", "#4a640d"];
const pollutants = { pm25: "PM2.5", pm10: "PM10", no2: "NO₂", o3: "O₃" };
const weatherVariables = {
  temperature_2m: { label: "Temperature", unit: "°C" },
  precipitation: { label: "Precipitation", unit: "mm" },
  relative_humidity_2m: { label: "Relative humidity", unit: "%" },
  wind_speed_10m: { label: "Wind speed", unit: "m/s" },
  pressure_msl: { label: "Sea-level pressure", unit: "hPa" },
};
const utcDate = new Intl.DateTimeFormat("en-GB", {
  timeZone: "UTC", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit",
});
const number = new Intl.NumberFormat("en", { maximumFractionDigits: 2 });

function utc(value) {
  return utcDate.format(new Date(value));
}

function fullUtc(value) {
  return new Date(value).toISOString().slice(0, 16).replace("T", " ") + " UTC";
}

function element(tag, text, className) {
  const node = document.createElement(tag);
  if (text !== undefined) node.textContent = text;
  if (className) node.className = className;
  return node;
}

function svgElement(tag, attributes, text) {
  const node = document.createElementNS("http://www.w3.org/2000/svg", tag);
  for (const [key, value] of Object.entries(attributes)) node.setAttribute(key, value);
  if (text !== undefined) node.textContent = text;
  return node;
}

function stationColor(node, index) {
  node.style.setProperty("--station", colors[index % colors.length]);
  return node;
}

async function loadReport(file) {
  const response = await fetch(file, { cache: "no-store" });
  if (!response.ok) throw new Error(`HTTP ${response.status} for ${file}`);
  return response.json();
}

function renderStations(report, latest) {
  const stations = report.locations.map((station, index) => {
    const card = stationColor(element("article", undefined, "panel station"), index);
    card.append(element("p", `${station.city} · OpenAQ #${station.location_id}`, "city"),
      element("h3", station.name));

    const currentStation = latest?.locations.find(({ id }) => id === station.location_id);
    const measurement = currentStation?.parameters.find(({ parameter }) =>
      parameter === report.parameter);
    const reading = element("p", measurement
      ? `${number.format(measurement.value)} ${measurement.unit}`
      : "—", "reading");
    const status = measurement?.status ?? (currentStation ? "missing" : "unavailable");
    const badge = element("span", status, "badge");
    badge.dataset.status = status;
    reading.append(badge);
    card.append(reading, element("p", measurement
      ? `Latest measurement · ${fullUtc(measurement.measured_at_utc)}`
      : `Latest ${pollutants[report.parameter]} measurement unavailable`, "note"));

    const coverage = station.coverage;
    const counts = element("dl");
    for (const [label, count] of [
      ["Finalized air quality", coverage.air_quality_hours],
      ["Complete model weather", coverage.complete_weather_hours],
      ["Both available", coverage.complete_join_hours],
    ]) {
      const row = element("div");
      row.append(element("dt", label), element("dd", `${count} / ${coverage.expected_hours} h`));
      counts.append(row);
    }
    card.append(counts);
    return card;
  });
  byId("stations").replaceChildren(...stations);
}

function renderChart(container, report, title, valueOf, selectedIndex) {
  const series = report.locations.map(({ hours }) => hours.map(valueOf));
  const values = series.flat().filter(Number.isFinite);
  if (values.length === 0) {
    container.replaceChildren(element("p", "No values in this period", "empty-chart"));
    return;
  }

  const left = 56, right = 510, top = 16, bottom = 142;
  const svg = svgElement("svg", { viewBox: "0 0 530 184", role: "img", "aria-label": title });
  const count = series[0].length;
  const low = Math.min(...values), high = Math.max(...values);
  const padding = high === low ? Math.max(Math.abs(high) * 0.1, 0.1) : (high - low) * 0.08;
  const min = low - padding, max = high + padding;
  // Reserve one hour of width after the last plotted hour start.
  const x = (index) => left + index / count * (right - left);
  const y = (value) => bottom - (value - min) / (max - min) * (bottom - top);

  for (let tick = 0; tick <= 4; tick++) {
    const value = min + (max - min) * tick / 4;
    svg.append(
      svgElement("line", { x1: left, x2: right, y1: y(value), y2: y(value), class: "grid-line" }),
      svgElement("text", { x: left - 6, y: y(value) + 4, "text-anchor": "end" }, number.format(value)),
    );
  }
  const start = Date.parse(report.start_utc);
  const tickIndices = count > 6 ? [0, Math.floor(count / 2), count] : [0, count];
  for (const index of tickIndices) {
    const anchor = index === 0 ? "start" : index === count ? "end" : "middle";
    svg.append(svgElement("text", { x: x(index), y: bottom + 25, "text-anchor": anchor },
      utc(start + index * hourMs)));
  }
  svg.append(svgElement("line", {
    x1: x(selectedIndex), x2: x(selectedIndex), y1: top, y2: bottom, class: "hour-marker",
  }));

  series.forEach((points, stationIndex) => {
    const color = colors[stationIndex % colors.length];
    const commands = [];
    const dots = [];
    points.forEach((value, index) => {
      if (!Number.isFinite(value)) return;
      const hasPrevious = Number.isFinite(points[index - 1]);
      const hasNext = Number.isFinite(points[index + 1]);
      commands.push(`${hasPrevious ? "L" : "M"} ${x(index)} ${y(value)}`);
      if ((!hasPrevious && !hasNext) || index === selectedIndex) {
        dots.push(svgElement("circle", { cx: x(index), cy: y(value), r: 3.5, fill: color }));
      }
    });
    svg.append(svgElement("path", {
      d: commands.join(" "), stroke: color, "stroke-width": 2, fill: "none",
    }), ...dots);
  });
  container.replaceChildren(svg);
}

function renderHour(report, selectedIndex, weatherKey) {
  const weather = weatherVariables[weatherKey];
  const { window_start_utc: start, window_end_utc: end } = report.locations[0].hours[selectedIndex];
  byId("hour-caption").textContent = `${fullUtc(start)} → ${fullUtc(end)}`;
  byId("weather-column").textContent = `${weather.label} (${weather.unit})`;
  const rows = report.locations.map((station) => {
    const row = element("tr");
    const heading = element("th", `${station.city} / ${station.name}`);
    heading.scope = "row";
    row.append(heading);
    const hour = station.hours[selectedIndex];
    const air = hour.air_quality;
    const weatherValue = hour.weather[weatherKey];
    row.append(
      element("td", air ? number.format(air.average_value) : "No finalized hour"),
      element("td", air ? number.format(air.measurement_count) : "—"),
      element("td", weatherValue ? number.format(weatherValue.value) : "Missing"),
      element("td", hour.weather_status),
    );
    return row;
  });
  byId("hour-values").replaceChildren(...rows);
}

function showHourly(report) {
  const pollutant = pollutants[report.parameter];
  if (report.version !== 1 || !pollutant || !report.locations[0]?.hours.length) {
    throw new Error("Unsupported hourly report");
  }
  const airTitle = `${pollutant} (${report.unit})`;
  byId("hourly-status").textContent =
    `Window ${fullUtc(report.start_utc)} → ${fullUtc(report.end_utc)} · ` +
    `Report generated ${fullUtc(report.generated_at_utc)}`;
  byId("air-title").textContent = airTitle;
  byId("air-column").textContent = `${airTitle} mean`;
  byId("weather-dataset").textContent = `Model dataset: ${report.weather_dataset}.`;
  byId("legend").replaceChildren(...report.locations.map((station, index) => {
    const item = stationColor(element("li"), index);
    item.append(element("span", undefined, "swatch"), document.createTextNode(station.name));
    return item;
  }));

  const hourPicker = byId("selected-hour");
  const weatherPicker = byId("weather-variable");
  weatherPicker.replaceChildren(...Object.entries(weatherVariables).map(([key, { label }]) =>
    new Option(label, key)));
  const hours = report.locations[0].hours;
  hourPicker.replaceChildren(...hours.map((hour, index) =>
    new Option(fullUtc(hour.window_start_utc), index)));
  const latestFinalized = hours.findLastIndex((_, index) =>
    report.locations.some((station) => station.hours[index].air_quality));
  hourPicker.selectedIndex = latestFinalized === -1 ? hours.length - 1 : latestFinalized;

  function draw() {
    const selectedIndex = hourPicker.selectedIndex;
    const key = weatherPicker.value;
    const { label, unit } = weatherVariables[key];
    const weatherTitle = `${label} (${unit})`;
    byId("weather-title").textContent = weatherTitle;
    byId("weather-note").textContent = key === "precipitation"
      ? "Total for the plotted hour, labelled at its end by Open-Meteo."
      : "Model value valid at the start of each plotted hour.";
    renderChart(byId("air-chart"), report, airTitle, (hour) => hour.air_quality?.average_value,
      selectedIndex);
    renderChart(byId("weather-chart"), report, weatherTitle, (hour) => hour.weather[key]?.value,
      selectedIndex);
    renderHour(report, selectedIndex, key);
  }

  hourPicker.addEventListener("change", draw);
  weatherPicker.addEventListener("change", draw);
  renderStations(report, null);
  draw();
  byId("dashboard").hidden = false;
}

async function main() {
  let hourly;
  try {
    hourly = await loadReport("hourly_environmental_insights.json");
    showHourly(hourly);
  } catch (error) {
    byId("hourly-status").textContent = "Hourly report unavailable. Generate the report and reload.";
    console.error(error);
    return;
  }

  try {
    const latest = await loadReport("air_quality_insights.json");
    renderStations(hourly, latest);
    byId("latest-status").textContent =
      `Latest report generated ${fullUtc(latest.generated_at_utc)} · ` +
      `Fresh within ${number.format(latest.freshness_threshold_seconds / 60)} min at generation`;
  } catch (error) {
    byId("latest-status").textContent = "Latest-measurement report unavailable.";
    console.error(error);
  }
}

main();
