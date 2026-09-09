# Week 1 Urban Data Lakehouse

Course project for ID2221 Week 1: build a reusable urban data integration platform.

## Minimal Platform

- Python
- PySpark
- Delta Lake
- YAML configuration
- Local filesystem lakehouse layout

## Project Layout

```text
configs/config.yaml     Dataset and storage configuration
src/pipeline.py         Thin command-line entrypoint
src/view_table.py       Delta table preview helper for PyCharm/terminal
src/common.py           Shared config, Spark, Delta IO, and timing helpers
src/data_consumption/   数据消费：source files -> raw Delta tables
src/data_cleaning/      数据清洗转化：raw -> normal Delta tables
src/data_integration/   数据联表：normal -> integrated_taxi_trips
src/data_aggregation/   数据聚合：integrated -> summary tables
src/data_analysis/      数据分析：benchmark and result analysis
tests/                  Lightweight tests
docs/report_notes.md    Notes for the final report
data/                   Raw data and generated Delta tables, ignored by Git
```

The data consumption step uses a source file registry. It tracks file path, size,
modified time, optional checksum, and ingestion status so interrupted runs can
restart without blindly re-consuming unchanged files.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
python -m src.pipeline raw
python -m src.pipeline normal
python -m src.pipeline integrated
python -m src.pipeline aggregate
python -m src.pipeline benchmark
```

Or run the whole main pipeline:

```bash
python -m src.pipeline all
```

## View Delta Tables

Use this helper instead of opening Delta table `part-*.parquet` files directly.
It reads the current Delta snapshot through `_delta_log`.

```bash
python -m src.view_table yellow_taxi_trips --layer raw --limit 10
python -m src.view_table raw/taxi_zone_lookup --limit 10
python -m src.view_table raw/yellow_taxi_trips --columns _dataset_name,_source_file,_source_file_size,_record_hash --limit 10
```

For large tables, skip the full row count:

```bash
python -m src.view_table raw/yellow_taxi_trips --limit 20 --no-count
```

The pipeline expects raw datasets under:

```text
data/raw/
  yellow_tripdata_2024-*.parquet
  taxi_zone_lookup.csv
  weather.csv
  hourly_88101_2024.csv
```

Generated Delta tables are written under:

```text
data/lakehouse/
  raw/          Raw Delta tables converted from source files
  normal/       Cleaned and standardized Delta tables
  integrated/   Joined analysis-ready tables
  aggregate/    Aggregated summary tables
  benchmark/    Tables used for storage strategy comparison
```

## Task B: Data Cleaning and Integration

Task B transforms the raw Delta tables into standardized normal tables and
builds the analysis-ready `integrated_taxi_trips` table.

The normal-table pipeline:

- validates non-null and unique primary keys for lookup and hourly tables,
- standardizes timestamps as local `timestamp_ntz` values,
- normalizes measurement types and replaces invalid measurements with nulls,
- removes taxi trips with invalid durations or timestamps outside Q1 2024,
- deduplicates taxi trips using the ingestion record hash,
- flags zero-distance trips, invalid distances, and financial adjustments,
- aggregates valid NYC PM2.5 observations into one city-level value per hour.

The integration pipeline enriches every retained taxi trip with:

- pickup and dropoff zones,
- pickup and dropoff boroughs,
- weather conditions at the pickup hour,
- average NYC PM2.5 at the pickup hour.

All enrichment steps use left joins so trips remain available when contextual
data is missing. Taxi zones, weather, and hourly air-quality data are broadcast
during integration because they are small relative to the taxi-trip fact table.

Run Task B with:

```bash
python -m src.pipeline normal
python -m src.pipeline integrated
```

Preview the outputs with:

```bash
python -m src.view_table normal/weather_hourly --limit 10
python -m src.view_table normal/air_quality_hourly --limit 10
python -m src.view_table integrated/integrated_taxi_trips --limit 10 --no-count
```

The current Q1 2024 run retains 9,551,387 cleaned taxi trips. The integrated
table has the same row count, with complete pickup-zone, dropoff-zone, weather,
and air-quality coverage. Unit tests cover primary-key validation, timestamp
normalization, hourly air-quality aggregation, and left-join preservation.

Run the tests with:

```bash
pytest -q
```
