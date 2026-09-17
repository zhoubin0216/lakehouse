"""Generate a standalone HTML dashboard from the Task 4 Delta products."""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
from html import escape
import json
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import create_spark, load_config, read_delta, table_path
from src.data_analysis.weather_conditions import weather_condition_name


DEFAULT_OUTPUT = Path("data/reports/task4_data_products_report.html")


def _product(spark: SparkSession, config: dict, name: str) -> DataFrame:
    definition = config["data_analysis"]["products"]["definitions"][name]
    path = table_path(config, definition["table"])
    if not Path(path).exists():
        raise FileNotFoundError(
            f"Missing analytical product '{name}' at {path}. "
            "Run `python -m src.data_analysis products` first."
        )
    return read_delta(spark, path)


def _iso(value: date | datetime) -> str:
    return value.isoformat()


def _weather_rows(frame: DataFrame) -> list[dict[str, Any]]:
    """Aggregate weather rows and support both v1 and v2 product schemas."""
    group_columns = ["weather_condition_code"]
    has_label = "weather_condition" in frame.columns
    if has_label:
        group_columns.append("weather_condition")
    rows = (
        frame.groupBy(*group_columns)
        .agg(F.sum("trip_count").alias("trip_count"))
        .orderBy(F.desc("trip_count"), F.asc("weather_condition_code"))
        .collect()
    )
    return [
        {
            "code": int(row.weather_condition_code),
            "condition": (
                row.weather_condition
                if has_label
                else weather_condition_name(int(row.weather_condition_code))
            ),
            "trips": int(row.trip_count),
        }
        for row in rows
    ]


def build_report_payload(spark: SparkSession, config: dict) -> dict[str, Any]:
    """Read the four products and return compact, browser-ready report data."""
    daily_frame = _product(spark, config, "daily_mobility_summary")
    zone_frame = _product(spark, config, "taxi_zone_statistics")
    weather_frame = _product(spark, config, "weather_impact_summary")
    air_frame = _product(spark, config, "air_quality_impact_summary")

    daily_rows = [
        {
            "date": _iso(row.pickup_date),
            "trips": int(row.trip_count),
            "revenue": float(row.total_revenue or 0.0),
            "peak_hour": int(row.peak_hour) if row.peak_hour is not None else None,
        }
        for row in (
            daily_frame.select(
                "pickup_date", "trip_count", "total_revenue", "peak_hour"
            )
            .orderBy("pickup_date")
            .collect()
        )
    ]
    if not daily_rows:
        raise ValueError("daily_mobility_summary is empty; the report cannot be generated")

    zone_rows = [
        {
            "zone": row.pickup_zone,
            "borough": row.pickup_borough,
            "trips": int(row.trip_count),
        }
        for row in (
            zone_frame.groupBy("pickup_zone", "pickup_borough")
            .agg(F.sum("trip_count").alias("trip_count"))
            .orderBy(F.desc("trip_count"), F.asc("pickup_zone"))
            .limit(10)
            .collect()
        )
    ]

    air_rows = [
        {
            "category": row.air_quality_category,
            "trips": int(row.trip_count),
            "avg_distance": float(row.avg_trip_distance or 0.0),
        }
        for row in (
            air_frame.groupBy("air_quality_category")
            .agg(
                F.sum("trip_count").alias("trip_count"),
                (
                    F.sum(F.col("avg_trip_distance") * F.col("trip_count"))
                    / F.sum("trip_count")
                ).alias("avg_trip_distance"),
            )
            .orderBy(F.desc("trip_count"))
            .collect()
        )
    ]

    total_trips = sum(row["trips"] for row in daily_rows)
    total_revenue = sum(row["revenue"] for row in daily_rows)
    busiest = max(daily_rows, key=lambda row: row["trips"])
    report_config = config.get("data_analysis", {}).get("report", {})
    generated_at = datetime.now(timezone.utc).replace(microsecond=0)

    return {
        "title": report_config.get("title", "Urban Mobility Data Products"),
        "subtitle": report_config.get("subtitle", "NYC Yellow Taxi"),
        "period": {
            "start": daily_rows[0]["date"],
            "end": daily_rows[-1]["date"],
        },
        "generated_at": generated_at.isoformat(),
        "kpis": {
            "total_trips": total_trips,
            "average_daily_trips": total_trips / len(daily_rows),
            "total_revenue": total_revenue,
            "busiest_date": busiest["date"],
            "busiest_day_trips": busiest["trips"],
            "busiest_day_peak_hour": busiest["peak_hour"],
        },
        "daily": daily_rows,
        "zones": zone_rows,
        "weather": _weather_rows(weather_frame),
        "air_quality": air_rows,
    }


def _safe_json(payload: dict[str, Any]) -> str:
    return (
        json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("&", "\\u0026")
    )


def render_report(payload: dict[str, Any]) -> str:
    """Render one complete, dependency-free HTML document."""
    title = escape(str(payload["title"]))
    subtitle = escape(str(payload["subtitle"]))
    data = _safe_json(payload)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <style>
    :root {{
      color-scheme: light dark;
      --bg: light-dark(#f3f6f8, #11171c);
      --surface: light-dark(#ffffff, #1a2229);
      --surface-alt: light-dark(#e9eff2, #222d35);
      --text: light-dark(#17242d, #edf3f6);
      --muted: light-dark(#62727d, #aab8c1);
      --border: light-dark(#d5dee3, #34434d);
      --navy: light-dark(#183f5b, #78b6db);
      --teal: light-dark(#148a88, #4fc1bc);
      --amber: light-dark(#c27713, #efb14d);
      --coral: light-dark(#bf5d4d, #ec8a79);
      --green: light-dark(#3d7d58, #78ba8d);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      letter-spacing: 0;
    }}
    main {{ max-width: 1380px; margin: 0 auto; padding: 28px; }}
    header {{
      display: flex;
      align-items: end;
      justify-content: space-between;
      gap: 24px;
      padding-bottom: 18px;
      border-bottom: 1px solid var(--border);
    }}
    h1, h2, p {{ margin: 0; }}
    h1 {{ font-size: 32px; font-weight: 650; }}
    h2 {{ font-size: 17px; font-weight: 650; }}
    .subtitle {{ color: var(--muted); margin-top: 5px; }}
    .period {{ color: var(--muted); text-align: right; font-size: 14px; }}
    .kpis {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin: 18px 0 22px;
    }}
    .kpi {{
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 7px;
      padding: 16px;
      min-width: 0;
    }}
    .kpi-label, .chart-note, footer {{ color: var(--muted); font-size: 13px; }}
    .kpi-value {{ font-size: 25px; font-weight: 650; margin: 7px 0 3px; font-variant-numeric: tabular-nums; }}
    .kpi-context {{ color: var(--muted); font-size: 12px; }}
    .charts {{ display: grid; grid-template-columns: 1.25fr 1fr; gap: 18px; }}
    .chart {{
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 7px;
      padding: 18px;
      min-width: 0;
    }}
    .chart-head {{ display: flex; justify-content: space-between; gap: 14px; align-items: baseline; margin-bottom: 12px; }}
    svg {{ display: block; width: 100%; overflow: visible; }}
    .axis {{ fill: var(--muted); font-size: 11px; }}
    .grid {{ stroke: var(--border); stroke-width: 1; }}
    .line {{ fill: none; stroke: var(--teal); stroke-width: 2.5; }}
    .area {{ fill: var(--teal); opacity: 0.12; }}
    .bar-zone {{ fill: var(--navy); }}
    .bar-weather {{ fill: var(--amber); }}
    .bar-air-good {{ fill: var(--green); }}
    .bar-air-other {{ fill: var(--coral); }}
    .value-label {{ fill: var(--text); font-size: 11px; font-variant-numeric: tabular-nums; }}
    .category-label {{ fill: var(--text); font-size: 11px; }}
    footer {{ margin-top: 18px; padding-top: 12px; border-top: 1px solid var(--border); }}
    code {{ color: var(--text); }}
    @media (max-width: 900px) {{
      .kpis {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      .charts {{ grid-template-columns: 1fr; }}
    }}
    @media (max-width: 560px) {{
      main {{ padding: 16px; }}
      header {{ display: block; }}
      .period {{ text-align: left; margin-top: 8px; }}
      .kpis {{ grid-template-columns: 1fr; }}
      .chart {{ padding: 14px; }}
      .chart-head {{ display: block; }}
      .chart-note {{ margin-top: 3px; }}
    }}
  </style>
</head>
<body>
  <main>
    <header>
      <div>
        <h1>{title}</h1>
        <p class="subtitle">{subtitle}</p>
      </div>
      <p class="period" id="period"></p>
    </header>

    <section class="kpis" aria-label="Key metrics">
      <article class="kpi"><div class="kpi-label">Total trips</div><div class="kpi-value" id="total-trips"></div><div class="kpi-context">Integrated trip records</div></article>
      <article class="kpi"><div class="kpi-label">Average daily demand</div><div class="kpi-value" id="average-daily"></div><div class="kpi-context">Trips per day</div></article>
      <article class="kpi"><div class="kpi-label">Total recorded revenue</div><div class="kpi-value" id="total-revenue"></div><div class="kpi-context">Sum of total_amount</div></article>
      <article class="kpi"><div class="kpi-label">Busiest day</div><div class="kpi-value" id="busiest-date"></div><div class="kpi-context" id="busiest-context"></div></article>
    </section>

    <section class="charts" aria-label="Data product charts">
      <article class="chart">
        <div class="chart-head"><h2>Daily taxi demand</h2><span class="chart-note">Trips per day</span></div>
        <svg id="daily-chart" role="img" aria-label="Daily taxi demand over the reporting period"></svg>
      </article>
      <article class="chart">
        <div class="chart-head"><h2>Highest-demand pickup zones</h2><span class="chart-note">Top 10 by trips</span></div>
        <svg id="zone-chart" role="img" aria-label="Ten pickup zones with the highest trip demand"></svg>
      </article>
      <article class="chart">
        <div class="chart-head"><h2>Demand by weather condition</h2><span class="chart-note">Meteostat condition names</span></div>
        <svg id="weather-chart" role="img" aria-label="Taxi trips grouped by named weather condition"></svg>
      </article>
      <article class="chart">
        <div class="chart-head"><h2>Demand by PM2.5 category</h2><span class="chart-note">Trip share and average distance</span></div>
        <svg id="air-chart" role="img" aria-label="Taxi trip share by air quality category"></svg>
      </article>
    </section>

    <footer>
      Generated from the four Task 4 Delta products. Weather names follow the Meteostat <code>coco</code> condition-code definition.
    </footer>
  </main>

  <script id="report-data" type="application/json">{data}</script>
  <script>
    (() => {{
      const data = JSON.parse(document.getElementById("report-data").textContent);
      const number = new Intl.NumberFormat("en-US");
      const compact = new Intl.NumberFormat("en-US", {{ notation: "compact", maximumFractionDigits: 1 }});
      const money = new Intl.NumberFormat("en-US", {{ style: "currency", currency: "USD", notation: "compact", maximumFractionDigits: 1 }});
      const shortDate = value => new Intl.DateTimeFormat("en-US", {{ month: "short", day: "2-digit" }}).format(new Date(value + "T00:00:00"));
      const fullDate = value => new Intl.DateTimeFormat("en-US", {{ year: "numeric", month: "short", day: "2-digit" }}).format(new Date(value + "T00:00:00"));
      const el = id => document.getElementById(id);
      const ns = "http://www.w3.org/2000/svg";

      el("period").textContent = `${{fullDate(data.period.start)}} to ${{fullDate(data.period.end)}}`;
      el("total-trips").textContent = compact.format(data.kpis.total_trips);
      el("average-daily").textContent = number.format(Math.round(data.kpis.average_daily_trips));
      el("total-revenue").textContent = money.format(data.kpis.total_revenue);
      el("busiest-date").textContent = shortDate(data.kpis.busiest_date);
      const peak = data.kpis.busiest_day_peak_hour;
      el("busiest-context").textContent = `${{number.format(data.kpis.busiest_day_trips)}} trips${{peak === null ? "" : `; peak hour ${{String(peak).padStart(2, "0")}}:00`}}`;

      function node(name, attrs = {{}}, text = null) {{
        const item = document.createElementNS(ns, name);
        Object.entries(attrs).forEach(([key, value]) => item.setAttribute(key, value));
        if (text !== null) item.textContent = text;
        return item;
      }}

      function setup(id, height) {{
        const svg = el(id);
        const width = Math.max(240, svg.parentElement.clientWidth - 36);
        svg.replaceChildren();
        svg.setAttribute("viewBox", `0 0 ${{width}} ${{height}}`);
        svg.setAttribute("height", height);
        return {{ svg, width, height }};
      }}

      function addTitle(mark, text) {{ mark.appendChild(node("title", {{}}, text)); }}

      function drawDaily() {{
        const {{ svg, width, height }} = setup("daily-chart", 285);
        const m = {{ top: 16, right: 16, bottom: 38, left: 58 }};
        const plotW = width - m.left - m.right;
        const plotH = height - m.top - m.bottom;
        const values = data.daily.map(d => d.trips);
        const min = Math.min(...values);
        const max = Math.max(...values);
        const pad = Math.max(1, (max - min) * 0.12);
        const yMin = Math.max(0, min - pad);
        const yMax = max + pad;
        const x = i => m.left + (data.daily.length === 1 ? plotW / 2 : i * plotW / (data.daily.length - 1));
        const y = value => m.top + (yMax - value) * plotH / (yMax - yMin || 1);

        for (let i = 0; i <= 4; i += 1) {{
          const value = yMin + (yMax - yMin) * i / 4;
          const py = y(value);
          svg.appendChild(node("line", {{ x1: m.left, x2: width - m.right, y1: py, y2: py, class: "grid" }}));
          svg.appendChild(node("text", {{ x: m.left - 8, y: py + 4, "text-anchor": "end", class: "axis" }}, compact.format(value)));
        }}
        const tickIndexes = [...new Set([0, Math.round((data.daily.length - 1) / 3), Math.round(2 * (data.daily.length - 1) / 3), data.daily.length - 1])];
        tickIndexes.forEach((index, position) => {{
          svg.appendChild(node("text", {{ x: x(index), y: height - 10, "text-anchor": position === 0 ? "start" : position === tickIndexes.length - 1 ? "end" : "middle", class: "axis" }}, shortDate(data.daily[index].date)));
        }});
        const linePoints = data.daily.map((d, i) => `${{x(i)}},${{y(d.trips)}}`).join(" ");
        const areaPoints = `${{m.left}},${{m.top + plotH}} ${{linePoints}} ${{width - m.right}},${{m.top + plotH}}`;
        svg.appendChild(node("polygon", {{ points: areaPoints, class: "area" }}));
        svg.appendChild(node("polyline", {{ points: linePoints, class: "line" }}));
        data.daily.forEach((d, i) => {{
          const hit = node("circle", {{ cx: x(i), cy: y(d.trips), r: 6, fill: "transparent" }});
          addTitle(hit, `${{fullDate(d.date)}}: ${{number.format(d.trips)}} trips`);
          svg.appendChild(hit);
        }});
      }}

      function drawHorizontalBars(id, rows, labelKey, barClass, height, tooltip) {{
        const {{ svg, width }} = setup(id, height);
        const labelW = Math.min(170, Math.max(116, width * 0.34));
        const m = {{ top: 8, right: 56, bottom: 24, left: labelW }};
        const plotW = width - m.left - m.right;
        const rowH = (height - m.top - m.bottom) / Math.max(rows.length, 1);
        const max = Math.max(...rows.map(d => d.trips), 1);
        rows.forEach((d, i) => {{
          const y = m.top + i * rowH + rowH * 0.18;
          const barH = rowH * 0.64;
          const barW = d.trips / max * plotW;
          const fullLabel = d[labelKey];
          const maxChars = Math.max(12, Math.floor(labelW / 7));
          const visibleLabel = fullLabel.length > maxChars ? `${{fullLabel.slice(0, maxChars - 3)}}...` : fullLabel;
          svg.appendChild(node("text", {{ x: m.left - 8, y: y + barH * 0.68, "text-anchor": "end", class: "category-label" }}, visibleLabel));
          const bar = node("rect", {{ x: m.left, y, width: barW, height: barH, class: barClass }});
          addTitle(bar, tooltip(d));
          svg.appendChild(bar);
          svg.appendChild(node("text", {{ x: m.left + barW + 6, y: y + barH * 0.68, class: "value-label" }}, compact.format(d.trips)));
        }});
      }}

      function drawAir() {{
        const chartHeight = Math.max(285, 170 + data.air_quality.length * 43);
        const {{ svg, width, height }} = setup("air-chart", chartHeight);
        const total = data.air_quality.reduce((sum, d) => sum + d.trips, 0);
        const m = {{ top: 46, right: 20, bottom: 28, left: 20 }};
        const plotW = width - m.left - m.right;
        let cursor = m.left;
        data.air_quality.forEach((d, index) => {{
          const w = plotW * d.trips / total;
          const rect = node("rect", {{ x: cursor, y: m.top, width: w, height: 72, class: index === 0 ? "bar-air-good" : "bar-air-other" }});
          addTitle(rect, `${{d.category}}: ${{number.format(d.trips)}} trips; ${{d.avg_distance.toFixed(2)}} mi average distance`);
          svg.appendChild(rect);
          if (w > 68) {{
            svg.appendChild(node("text", {{ x: cursor + w / 2, y: m.top + 31, "text-anchor": "middle", class: "value-label" }}, d.category.replaceAll("_", " ")));
            svg.appendChild(node("text", {{ x: cursor + w / 2, y: m.top + 51, "text-anchor": "middle", class: "value-label" }}, `${{(100 * d.trips / total).toFixed(1)}}%`));
          }}
          cursor += w;
        }});
        data.air_quality.forEach((d, i) => {{
          const y = 158 + i * 43;
          svg.appendChild(node("circle", {{ cx: m.left + 6, cy: y - 4, r: 6, class: i === 0 ? "bar-air-good" : "bar-air-other" }}));
          svg.appendChild(node("text", {{ x: m.left + 22, y, class: "category-label" }}, `${{d.category.replaceAll("_", " ")}}: ${{compact.format(d.trips)}} trips`));
          svg.appendChild(node("text", {{ x: width - m.right, y, "text-anchor": "end", class: "value-label" }}, `${{d.avg_distance.toFixed(2)}} mi avg distance`));
        }});
      }}

      function drawAll() {{
        drawDaily();
        drawHorizontalBars("zone-chart", data.zones, "zone", "bar-zone", 285, d => `${{d.zone}}, ${{d.borough}}: ${{number.format(d.trips)}} trips`);
        drawHorizontalBars("weather-chart", data.weather, "condition", "bar-weather", Math.max(285, data.weather.length * 34 + 32), d => `${{d.condition}} (code ${{d.code}}): ${{number.format(d.trips)}} trips`);
        drawAir();
      }}

      drawAll();
      let resizeTimer;
      window.addEventListener("resize", () => {{
        clearTimeout(resizeTimer);
        resizeTimer = setTimeout(drawAll, 100);
      }});
    }})();
  </script>
</body>
</html>
"""


def generate_data_product_report(
    spark: SparkSession,
    config: dict,
    output_path: Path | str | None = None,
) -> Path:
    """Generate the report and return its resolved output path."""
    configured = config.get("data_analysis", {}).get("report", {}).get("output_file")
    destination = Path(output_path or configured or DEFAULT_OUTPUT)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        render_report(build_report_payload(spark, config)),
        encoding="utf-8",
    )
    return destination.resolve()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/config.yaml"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    config = load_config(args.config)
    spark = create_spark()
    try:
        output = generate_data_product_report(spark, config, args.output)
        print(f"Generated data product report: {output}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
