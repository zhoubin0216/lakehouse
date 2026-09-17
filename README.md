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
src/data_analysis/      queries, reusable products, and optimization benchmarks
tests/                  Lightweight tests
docs/report_notes.md    Notes for the final report
docs/architecture.md    Project architecture diagrams
docs/week2_task4.md     Task 4 product design and operating notes
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

(`data/lakehouse/benchmark/benchmark_results.csv`).

# Week 2 Urban Data Lakehouse

Course project for ID2221 Week 2: Querying and Optimizing the Urban Data Platform.

## Analytical Queries

This step executes six reusable Spark SQL analytical queries using the
integrated taxi-trip dataset and the underlying Delta tables.

Run the analytical queries with:

```bash
python -m src.data_analysis.query_benchmark
```

The query results are saved as Delta tables under:

```text
data/lakehouse/analysis/baseline_queries/
```

Each query is run once as a warm-up and five times for measurement.
Individual execution times and summary statistics are saved to:

```text
data/lakehouse/analysis/baseline_queries/baseline_query_times.csv
```

The median execution time is used as the baseline query latency for
subsequent performance comparisons.

## Controlled Optimization Benchmark (Week 2 Tasks 3 and 5)

The original query library is unchanged. The controlled baseline explicitly
disables AQE, automatic broadcast, and adaptive broadcast; Spark table caches
are cleared at variant boundaries. This baseline is different from the earlier
default-configuration `query_benchmark` run. Baseline still uses existing Delta
partitioning, column pruning, and other normal Spark optimizations.

Run all six queries with one warm-up and five measured executions:

```bash
.venv/bin/python -m src.data_analysis.optimization_benchmark
```

Compare Q1 against caching the full integrated table:

```bash
.venv/bin/python -m src.data_analysis.optimization_benchmark \
  --query monthly_zone_demand --variant baseline cache
```

Omit `--query` to compare all six queries. The cache is built once and shared
across the selected queries; its build cost is NOT included in query latency.
It uses `MEMORY_AND_DISK`, and actual cached memory/disk bytes and cached
partition counts are recorded. A cache variant automatically includes baseline.
The framework's `Variant` interface supports future Spark setting overrides,
cache views, and equivalent SQL overrides. Available variants: baseline,
cache, broadcast, aqe, combined. Broadcast hints affect Q3/Q4/Q6; other queries
have no applicable join and their broadcast arms are no-op controls.

Settings are under `data_analysis.optimization_benchmark` in YAML. Each run
creates a unique UTC timestamp/ID directory; previous outputs are not overwritten:

```text
data/lakehouse/analysis/optimization/<run_id>/
  environment.json          Versions, Git state, methodology, pinned Delta inputs
  status.json               running / completed / failed
  timings.csv               Every measured execution
  benchmark_results.csv     Median, mean, min/max, speedup, plan paths
  validation_results.csv    Every run checked against baseline and its variant
  <variant>/
    configuration.json      Effective Spark settings and cache cost/storage
    <query>.sql             Exact SQL used
    plans/<query>_initial.txt
    plans/<query>_final.txt  Formatted plan of the SAME DataFrame after collect()
    metrics/<query>_run_N.json  Runtime SQL metrics, scans, joins and AQE stages
    results/<query>.json     Schema, duplicate-preserving hash, canonical rows
```

Delta input versions are pinned for the entire experiment. Input sizes and file
counts come from the active Delta snapshot, not obsolete historical files.
Elapsed time includes `spark.sql()` construction and `collect()`, excluding
input registration, cache build, validation and plan export. Results must be
small analytical outputs: they are collected to the driver, and the configured
row cap is checked AFTER collection (not a memory guarantee for arbitrary SQL).

Validation ignores output order but preserves duplicates. Ordered column names
and SQL types must match; nullability/metadata may differ. Integers, decimals,
strings, dates and other non-floating values are exact. Top-level float/double
columns use configured absolute/relative tolerances, with explicit NaN/null
handling. Exact hashes may differ while the tolerance check passes. A failed
check stops the experiment with a failed status and diagnostic validation CSV.

These are warmed-access measurements: OS file cache is not cleared. The individual
CLI uses baseline-first order. A correctness reference is collected before timing;
this reference is separate from measured baseline runs. A speedup below 1 is valid.

### Complete pre-Task-4 experiment suite

```bash
.venv/bin/python -m src.data_analysis.optimization_suite
```

This runs serially; do NOT run tests/other Spark jobs while it measures performance.
Allow several minutes. All tables are read-only and original Task 1/2 SQL is unchanged.
The suite performs:

1. Three seeded, randomized-order blocks of all six queries under baseline,
   full-table cache, explicit broadcast, AQE-only and broadcast+AQE.
2. Three blocks of January Q1: function-based time filter and timestamp-range
   filter, each with/without equivalent year/month partition predicates. The
   range-filter control captures any benefit already provided by Delta statistics.
3. Three blocks of AQE off/on for Q1/Q4 with the SAME 32 initial shuffle partitions
   in both arms, separately from the main 4-partition experiment.
4. Frozen candidate selection (>=5% median improvement and majority block wins,
   applicable variants only), followed by three independent confirmation blocks.
   If none qualifies, baseline is retained. Confirmation regressions are reported.

Each block has one warm-up and five timed runs; no formal significance claim is
made from this small number of blocks. AQE dynamic broadcast stays disabled so
AQE and explicit broadcast effects are separated. Cache build cost and actual
memory/disk placement are recorded separately. Pruning adds equivalent predicates
only after checking partition/date consistency on the complete pinned dataset.

Outputs are under `data/lakehouse/analysis/optimization/suites/<suite_id>/`:

- `aggregate_results.csv`: all configurations pooled across blocks.
- `final_query_comparison.csv`: six-query independent confirmation summary.
- `all_timings.csv`, `all_validations.csv`: auditable per-run evidence.
- `plan_evidence.json`: representative runtime scans, joins and AQE changes.
- `suite_manifest.json`: seed, environment, snapshot versions, order and job paths.
- `benchmark_report.md`: measured methodology, results, trade-offs and evaluation.

A compact, Git-visible report/evidence bundle is exported to
`docs/benchmarks/week2/<suite_id>/` (unique directory, no overwriting). It includes
representative SQL, configuration, initial/final formatted plans and runtime
metrics. The ignored `data/` output does not need to be committed or uploaded.
`--export-root` and `--output-root` can customize destinations; `--blocks 1` is a
smoke run, not the default three-block report. See `suite` settings in YAML.

Task 4 product creation, storage overhead, refresh timing and product-vs-on-demand
comparisons are kept outside this optimization suite. Existing weather codes and
Q4 semantics should be confirmed with their owner before final submission.

## Reusable Analytical Data Products (Week 2 Task 4)

Task 4 is an independent analytical workload and is not part of
`python -m src.pipeline all`. Refresh all four products from one pinned version
of the Integrated Delta table:

```bash
.venv/bin/python -m src.data_analysis products
```

Refresh one product:

```bash
.venv/bin/python -m src.data_analysis products \
  --product taxi_zone_statistics
```

The following products are written under `data/lakehouse/analysis/products/`:

| Product | Grain | Intended use |
|---|---|---|
| `daily_mobility_summary` | pickup date | daily operational mobility reporting |
| `taxi_zone_statistics` | pickup month and taxi zone | zone planning and taxi operations |
| `weather_impact_summary` | pickup month and named weather condition | weather-impact comparisons |
| `air_quality_impact_summary` | pickup month and PM2.5 band | environmental mobility analysis |

The products are deliberately unpartitioned because the aggregations are small;
pre-aggregation provides the main query-speed benefit without creating tiny
partition files. Every product row records its product schema version, source
Integrated table and Delta version, source schema-version snapshot, creation
time, and refresh time.

`analysis/product_catalog` stores one row per product with its users, grain,
materialization reason, source snapshot, first creation and latest refresh time,
row count, active Delta storage bytes, refresh duration, and partition strategy.
Refreshing one product preserves the catalog records for the other products.

The PM2.5 categories are stable project-defined analytical bands used to make
reports comparable; they are not presented as health advice. Query execution is
also available through the independent analysis entry:

```bash
.venv/bin/python -m src.data_analysis queries
```

`weather_impact_summary` keeps the source `weather_condition_code` and adds the
human-readable `weather_condition` value defined by Meteostat, such as `Clear`,
`Overcast`, or `Light Rain`. The mapping in
`src/data_analysis/weather_conditions.py` is shared by products and reports.

Generate a standalone visual report from the four product tables:

```bash
.venv/bin/python -m src.data_analysis report
```

The default output is `data/reports/task4_data_products_report.html`. It embeds
the aggregated report data and rendering code, so it can be opened directly in
a browser without a web server or external JavaScript packages. Use `--output`
to select another destination.

Preview a product or its catalog through the existing Delta viewer:

```bash
.venv/bin/python -m src.view_table analysis/products/daily_mobility_summary --limit 10
.venv/bin/python -m src.view_table analysis/product_catalog --limit 10
```

Run the test suite:

```bash
.venv/bin/pytest -q
```
