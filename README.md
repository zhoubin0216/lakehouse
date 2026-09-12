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
src/data_quality.py     Accepted/rejected record classification and storage
src/data_consumption/   source files -> raw Delta tables
src/data_cleaning/      raw -> normal Delta tables
src/data_integration/   normal -> integrated_taxi_trips
src/data_aggregation/   integrated -> summary tables
src/data_analysis/      benchmark and result analysis
tests/                  Lightweight tests
docs/report_notes.md    Notes for the final report
docs/architecture.md    Project architecture diagrams
data/                   Raw data and generated Delta tables, ignored by Git
```

The data consumption step uses a source file registry. It tracks file path, size,
modified time, optional checksum, schema version, and ingestion status so
interrupted runs can restart without blindly re-consuming unchanged files.

Each dataset declares a positive integer `current_schema_version` pointer and a
`schema_versions` mapping in `configs/config.yaml`. The mapping preserves every
source schema contract, while the pointer selects the contract used for new
ingestion. Every newly consumed raw record stores the selected version in
`_schema_version`; the same value is also stored in the ingestion-run metadata
and the source-file registry. Increment the pointer only when the source schema
contract or its interpretation changes.

Changing `current_schema_version` does not itself re-consume unchanged source
files or rewrite historical raw rows. Existing rows retain the version under
which they were ingested, while new or changed files use the current version.
Rebuilding history is a separate, explicit backfill operation.

Version-specific source parsing and mapping settings are nested by version:

```yaml
current_schema_version: 2
schema_versions:
  "1":
    format: csv
    read_options: {header: true, inferSchema: false}
    expected_columns: [old_name]
    column_types: {old_name: int}
    columns: {old_name: canonical_name}
  "2":
    format: csv
    read_options: {header: true, inferSchema: false}
    expected_columns: [new_name]
    column_types: {new_name: int}
    columns: {new_name: canonical_name}
```

Each schema version must define one Spark SQL type in `column_types` for every
expected source column. Parquet physical types must match exactly. CSV files are
read without `inferSchema`; non-empty values that cannot be safely converted are
written to `rejected/consumption/<dataset>` with `_rejection_reasons`, while
missing or unexpected columns fail the ingestion run as a file-level contract
violation.

Raw tables created before `column_types` was introduced use CSV-inferred physical
types. Recreate those raw tables and their source-file registries together before
the next real ingestion run; after that one-time migration, incremental writes
use the explicit contract types.

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
```

Run the incremental main pipeline (the intended scheduled-task entrypoint):

```bash
python -m src.pipeline all
```

This command always checks the raw sources first. It runs `normal`, `integrated`,
and `aggregate` in order only when consumption writes at least one new accepted
raw record. If no valid new data is found, the three downstream steps are skipped.
Rejected-only input is recorded but does not trigger a downstream rebuild.

Benchmarking is intentionally outside the recurring pipeline. Run it explicitly:

```bash
python -m src.data_analysis.benchmark
```

## View Delta Tables

Use this helper instead of opening Delta table `part-*.parquet` files directly.
It reads the current Delta snapshot through `_delta_log`.

```bash
python -m src.view_table yellow_taxi_trips --layer raw --limit 10
python -m src.view_table raw/taxi_zone_lookup --limit 10
python -m src.view_table raw/yellow_taxi_trips --columns _dataset_name,_schema_version,_source_file,_source_file_size,_record_hash --limit 10
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
  rejected/     Consumption and cleaning rejected-record tables
```

## Data Cleaning and Integration

This step transforms the raw Delta tables into standardized normal tables and
builds the analysis-ready `integrated_taxi_trips` table.

The normal-table pipeline:

- resolves repeated business keys by preferring the highest source schema version
  and then the latest ingestion timestamp,
- preserves `source_schema_version` for one-record outputs and
  `source_schema_versions` for hourly air-quality aggregates,
- writes records that fail required-key, timestamp, project-period, duration,
  measurement, or unit rules to `rejected/cleaning/<dataset>`,
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

The integrated table keeps source-specific lineage instead of assigning one
ambiguous global schema version: `taxi_schema_version`,
`pickup_zone_schema_version`, `dropoff_zone_schema_version`,
`weather_schema_version`, and `air_quality_schema_versions`.

All enrichment steps use left joins so trips remain available when contextual
data is missing. Taxi zones, weather, and hourly air-quality data are broadcast
during integration because they are small relative to the taxi-trip fact table.

Run this step with:

```bash
python -m src.pipeline normal
python -m src.pipeline integrated
```

Preview the outputs with:

```bash
python -m src.view_table normal/weather_hourly --limit 10
python -m src.view_table normal/air_quality_hourly --limit 10
python -m src.view_table integrated/integrated_taxi_trips --limit 10 --no-count
python -m src.view_table rejected/consumption/weather_hourly --limit 10
python -m src.view_table rejected/cleaning/yellow_taxi_trips --limit 10
```

The current Q1 2024 run retains 9,551,387 cleaned taxi trips. The integrated
table has the same row count, with complete pickup-zone, dropoff-zone, weather,
and air-quality coverage. Unit tests cover primary-key validation, timestamp
normalization, hourly air-quality aggregation, and left-join preservation.

Run the tests with:

```bash
pytest -q
```

## Data Aggregation and Benchmarking

This step builds analytical summary tables from `integrated_taxi_trips` and
benchmarks different Delta Lake storage strategies for the taxi-trip data.

The aggregation pipeline creates three Delta summary tables:

- number of taxi trips per pickup borough,
- average trip duration per pickup date,
- average fare amount per pickup borough.

Run the aggregation step with:

```bash
python -m src.pipeline aggregate
```

The benchmark compares four main Delta storage strategies using the same taxi-trip
dataset:

- unpartitioned,
- partitioned by `pickup_borough`,
- partitioned by `pickup_month`,
- partitioned by `pickup_date`.

Three additional random file-count control strategies are also evaluated:

- random partitioning into 20 files,
- random partitioning into 128 files,
- random partitioning into 608 files.

These controls match the Parquet file counts produced by the `pickup_month`,
`pickup_borough`, and `pickup_date` strategies respectively, allowing the effect
of file count to be compared independently from the semantic partitioning column.

For each strategy, the benchmark measures Delta write/ingestion time, storage size,
generated file count, and query latency. Each strategy is written three times,
and the median write time is used for comparison. Each required query is run
once as a warm-up and then five times for measurement, with the median latency
reported.

The benchmark executes the following required queries:

- number of taxi trips per pickup borough,
- average trip duration per pickup date,
- average fare amount per pickup borough.

Run the independent benchmark with:

```bash
python -m src.data_analysis.benchmark
```

The generated benchmark Delta tables are written under:

```text
data/lakehouse/benchmark/
  taxi_unpartitioned/
  taxi_by_pickup_borough/
  taxi_by_pickup_month/
  taxi_by_pickup_date/
  taxi_random_20_files/
  taxi_random_128_files/
  taxi_random_608_files/
```

Benchmark statistics are saved to the configured CSV result file
(`data/lakehouse/benchmark/benchmark_results.csv`). Each result row includes a
`schema_versions` JSON snapshot. Benchmark Delta tables also retain the
source-specific schema-version columns used by their input records.
