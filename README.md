# Urban Data Lakehouse (Weeks 1-3)

Course project for ID2221. Week 1 builds the reusable Delta Lake integration
platform; Week 2 adds Spark SQL analytics, controlled optimization experiments,
reusable analytical products, and a standalone HTML report.

## Week 3: Incremental Updates (Tasks 1-2)

After the initial raw, normal and integrated tables have been built:

```bash
python -m src.incremental.generate
python -m src.pipeline updates --manifest data/raw/updates/week3_release2/manifest.json
python -m src.incremental.verify
```

The generator creates one immutable update file per dataset and a manifest with
record counts, schema versions and SHA-256 checksums. Taxi adds 6% new trips and
1.5% duplicates, both relative to the original raw row count. Weather and air
quality add the following week's hourly records with numeric `humidity` and
`aqi`. The unchanged zone lookup has a header-only CSV. These are synthetic
fixtures, not new real observations or official AQI calculations.

`python -m src.pipeline updates` discovers all release manifests. Repeating the
command resumes an interrupted release or skips completed releases without
Delta writes. `all` also discovers releases after checking the original inputs.
Keep the original source files immutable; deliver corrections as a new release.
Use `updates` for regular Week 3 operation, not the manual full-build commands.

The update path deduplicates against existing records, appends raw revisions,
merges changed normal/integrated records, refreshes affected aggregate groups,
and refreshes only dependent Week 2 products for affected dates/months.
Benchmark execution remains separate. The HTML report is a static export;
regenerate it with the existing report command after updating Delta products.

State, pinned baseline versions, counts, errors and replay staging tables are
stored under `data/metadata/updates/<release_id>/`. Do not delete this state or
vacuum its baseline snapshots before an interrupted release finishes. Local
updates use a single-writer lock; do not run manual table rebuilds concurrently.

Version 1 remains defined. Original weather/air files explicitly use
`source_schema_version: 1`; the current contract is version 2, and every update
file declares its own version in the manifest. Old rows are not rebuilt merely
because the schema pointer changes. New nullable fields read as null on old rows.

Implementation and Task 2 design discussion: [docs/week3_tasks1_2.md](docs/week3_tasks1_2.md).

## Week 3: Operational Monitoring (Task 3)

Task 3 adds operational monitoring around the incremental update pipeline. The
monitoring layer records both release-level executions and per-dataset update
metrics in Delta tables so pipeline behavior can be inspected with Spark SQL.
Monitoring is best-effort: a monitoring write failure is reported as a warning
but does not invalidate a successful business-data update.

Enable monitoring in `configs/config.yaml`:

```yaml
monitoring:
  enabled: true
  pipeline_runs_table: monitoring/pipeline_runs
  schema_events_table: monitoring/schema_events
```

The main monitoring table is `monitoring/pipeline_runs`. It records:

- run and parent-run identifiers,
- release ID, operation type, pipeline step, dataset, and target table,
- execution status and UTC start/finish timestamps,
- processing duration,
- processed, inserted, duplicate, and rejected record counts,
- validation-failure count,
- source schema version,
- error type/message and compact JSON details when available.

Incremental releases use three semantic levels. `pipeline_command` records the
CLI command, `incremental_release` records one release attempt, and
`dataset_update` records each dataset handled by that attempt. Completed releases
are skipped without Delta rewrites. During resume, datasets already completed in
the persisted release state are marked `SKIPPED`; newly executed datasets are
marked `SUCCESS`, and failures are retained as `FAILED` records.

For dataset updates, `processed_records` is the number of source records
attempted, including consumption-stage rejected records. `inserted_records` is
the number of novel accepted raw records appended after duplicate detection.
`rejected_records` combines consumption-stage, cleaning-stage, conflicting
revision, and missing-reference rejects. Exact duplicates are reported
separately in `duplicate_records` even though their rows are also preserved in
the deduplication quarantine.
Because cleaning rejects are a subset of rows already inserted into the raw
layer, `processed_records` is not expected to equal
`inserted_records + duplicate_records + rejected_records`.

`validation_failures` counts failed validation rules, not only rejected rows. A
single rejected record may therefore contribute more than one validation
failure. Detailed rejected rows and their `_rejection_reasons` remain in the
existing rejected Delta tables.

Schema-version transitions are recorded separately in
`monitoring/schema_events`. The table stores the old and new schema versions,
added/removed columns, changed types, compatibility, release ID, and detection
time. The current Week 3 fixture records the additive weather `humidity` and air
quality `aqi` changes from schema version 1 to version 2.

Run an incremental release and then execute all monitoring queries:

```bash
python -m src.pipeline updates --manifest data/raw/updates/week3_release2/manifest.json
python -m src.pipeline monitoring
```

The monitoring query command executes the Task 3 Spark SQL files under
`src/monitoring/sql/`:

| Query | Purpose |
|---|---|
| `validation_failures_by_dataset` | Compare validation failures and rejected records by dataset |
| `processing_time_by_dataset` | Compare average, maximum, and minimum dataset processing time |
| `rejected_records_by_run` | Inspect rejected records and validation failures for each dataset execution |
| `processing_time_trend` | Inspect successful processing time over repeated executions |
| `schema_evolution_history` | Inspect recorded schema-version changes |

List or run monitoring queries directly:

```bash
python -m src.monitoring.queries --list
python -m src.monitoring.queries --query processing_time_by_dataset
```

Preview the persisted monitoring tables with the existing Delta viewer:

```bash
python -m src.view_table monitoring/pipeline_runs --limit 100 --no-count
python -m src.view_table monitoring/schema_events --limit 100 --no-count
```

The Delta tables are stored under:

```text
data/lakehouse/monitoring/
  pipeline_runs/
  schema_events/
```

A deterministic validation fixture can be generated to demonstrate that invalid
records are rejected and counted without failing the whole release. Use a fresh
release ID because generated releases are immutable:

```bash
python -m src.incremental.generate \
  --release week3_validation_test \
  --inject-invalid
python -m src.pipeline updates \
  --manifest data/raw/updates/week3_validation_test/manifest.json
python -m src.pipeline monitoring
```

The validation fixture injects one out-of-range weather humidity value and one
out-of-range AQI value. These records are expected to appear in the rejected
record counts while the valid part of the release continues normally.

For production monitoring, useful derived signals include processing latency and
throughput trends, rejected-record and validation-failure rates, duplicate rate,
failed-run frequency, schema-change events, and repeated dataset retries. These
metrics can be used for dashboards and threshold-based alerts, while the Delta
history provides an auditable execution record.

## Week 3: Extensible Validation (Task 4)

Task 4 adds one validation framework shared by full builds and incremental
updates. A bad row no longer needs to stop a whole dataset: valid rows continue,
while invalid rows are excluded from downstream tables and written to a staged
Delta quarantine with `_validation_rule_ids`, `_validation_categories`,
`_rejection_reasons`, `_rejection_stage`, and `_rejected_at`.

The framework detects:

- exact duplicates and conflicting revisions,
- invalid numeric, timestamp, duration, and configured-period values,
- incomplete business keys and other required fields,
- taxi trips whose pickup or dropoff zone does not exist,
- unsupported source columns, physical types, and manifest schema versions.

Quarantine tables are stored by validation stage:

```text
data/lakehouse/rejected/
  consumption/<dataset>/
  cleaning/<dataset>/
  deduplication/<dataset>/
  reference/<dataset>/
```

Generate the cross-dataset validation report after a full build or update:

```bash
python -m src.pipeline validation
python -m src.view_table validation/rule_summary --limit 100 --no-count
```

The report groups failed records by dataset, stage, rule ID, category, and
human-readable reason. Unsupported schema changes are intentionally handled as
structured contract errors rather than row rejects, because the platform cannot
safely interpret rows until a new schema version is registered.

Generic rule factories such as required, non-empty, range, and minimum checks
are reusable across datasets. Dataset-specific behavior is registered with a
declarative predicate or reference rule; extending validation does not require
editing the classification, quarantine, or reporting engine.

Implementation details and the generic-versus-specific design discussion:
[docs/week3_task4_validation.md](docs/week3_task4_validation.md).

## Week 3: Production-Readiness Evaluation (Task 5)

Task 5 provides a controlled, reproducible evaluation suite for the Week 3
platform. It measures duplicate-aware incremental insertion, scoped refresh of
all four analytical products, validation overhead, monitoring overhead, and the
physical storage occupied by core and supporting platform components.

Run the configured experiment:

```bash
python -m src.evaluation
```

For a quicker smoke run or a Git-visible report:

```bash
python -m src.evaluation \
  --rows 2000 \
  --repeats 2 \
  --export-report docs/benchmarks/week3/production_readiness.md
```

Each execution creates a unique directory under `data/evaluation/week3/` with:

```text
measurements.json       Full measurements and environment metadata
measurements.csv        Compact timing table
evaluation_report.md    Short evaluation report and maintainability discussion
artifacts/              Isolated Delta inputs, outputs, quarantine and monitoring data
```

The synthetic experiment never modifies the configured municipality tables.
It provides repeatable component measurements on any machine. The incremental
release pipeline also persists real `integration_refresh_seconds`,
`aggregate_refresh_seconds`, per-product refresh durations, and total
`analytical_refresh_seconds` in release state. When a completed release exists,
the evaluation report includes those real workload timings alongside the
controlled experiment.

Storage figures are filesystem bytes, including Delta logs and retained table
history. Validation overhead compares the same accepted/rejected split with
zero rules and with three representative rules. Monitoring overhead compares
the same cached Spark aggregation with monitoring disabled and with one Delta
metadata append. Results are warmed local measurements, not claims about
cluster-scale throughput or statistical significance.

Methodology and design discussion:
[docs/week3_task5_evaluation.md](docs/week3_task5_evaluation.md).

Measured local example:
[docs/benchmarks/week3/production_readiness.md](docs/benchmarks/week3/production_readiness.md).
The completed real-release counts and timings are summarized in
[docs/benchmarks/week3/release2_evidence.md](docs/benchmarks/week3/release2_evidence.md).

## Week 1: Data Platform

### Minimal Platform

- Python
- Java 17
- PySpark
- Delta Lake
- YAML configuration
- Local filesystem lakehouse layout

### Project Layout

```text
configs/config.yaml     Dataset and storage configuration
src/pipeline.py         Thin command-line entrypoint
src/view_table.py       Delta table preview helper for PyCharm/terminal
src/common.py           Shared config, Spark, Delta IO, and timing helpers
src/data_quality.py     Extensible validation, quarantine, and rule reporting
src/data_consumption/   source files -> raw Delta tables
src/data_cleaning/      raw -> normal Delta tables
src/data_integration/   normal -> integrated_taxi_trips
src/data_aggregation/   integrated -> summary tables
src/data_analysis/      Week 2 query, optimization, product, and report modules
src/evaluation/         Week 3 production-readiness measurements and report
  sql/                  Six reusable Spark SQL query files
  query_library.py      Ordered query registry
  query_benchmark.py    Default-configuration baseline measurements
  optimization_*.py     Controlled Task 3/5 experiments and evidence export
  data_products.py      Four materialized Delta products
  data_product_report.py
                        Standalone HTML report generator
src/incremental/        Update generation, restartable releases, scoped refresh
tests/                  Lightweight tests
docs/report_notes.md    Notes for the final report
docs/architecture.md    Project architecture diagrams
docs/week2_task4.md     Task 4 product design and operating notes
data/                   Local inputs, Delta tables, benchmark outputs, and reports
```

The data consumption step uses a source file registry. It tracks file path, size,
modified time, optional checksum, schema version, and ingestion status so
interrupted runs can restart without blindly re-consuming unchanged files.

Each dataset declares a positive integer `current_schema_version` pointer and a
`schema_versions` mapping in `configs/config.yaml`. The mapping preserves every
source schema contract, while the pointer selects the default contract for new
ingestion. Original-file `source_schema_version` and release manifest versions
override that default explicitly. Every newly consumed raw record stores its selected version in
`_schema_version`; the same value is also stored in the ingestion-run metadata
and the source-file registry. Increment the pointer only when the source schema
contract or its interpretation changes.

Changing `current_schema_version` does not itself re-consume unchanged source
files or rewrite historical raw rows. Existing rows retain the version under
which they were ingested, while new releases use their declared versions.
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

### Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Run

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

### View Delta Tables

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
  rejected/     Consumption, cleaning, deduplication, and reference quarantines
  validation/   Rule-level validation summary
```

### Data Cleaning and Integration

This step transforms the raw Delta tables into standardized normal tables and
builds the analysis-ready `integrated_taxi_trips` table.

The normal-table pipeline:

- resolves repeated business keys by latest ingestion timestamp, with source
  schema version as a tie-breaker rather than a record revision number,
- preserves `source_schema_version` for one-record outputs and
  `source_schema_versions` for hourly air-quality aggregates,
- writes records that fail required-key, timestamp, project-period, duration,
  measurement, or unit rules to `rejected/cleaning/<dataset>`,
- validates non-null and unique primary keys for lookup and hourly tables,
- standardizes timestamps as local `timestamp_ntz` values,
- normalizes measurement types and quarantines non-null values outside the
  supported ranges,
- removes taxi trips with invalid durations or timestamps outside configured coverage periods,
- deduplicates taxi trips using the ingestion record hash,
- flags zero-distance trips and financial adjustments, while quarantining
  distances outside the configured range,
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

Weather and air-quality enrichment use left joins so trips remain available when
optional contextual observations are missing. Pickup and dropoff taxi zones are
required references; unknown IDs are quarantined before integration. Taxi zones,
weather, and hourly air-quality data are broadcast during integration because
they are small relative to the taxi-trip fact table.

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

### Data Aggregation and Benchmarking

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

## Week 2: Querying and Optimization

Week 2 is implemented under `src/data_analysis/` and runs independently from
the recurring Week 1 pipeline. Before running it, build the Week 1 `normal` and
`integrated` tables. Do not run multiple Spark benchmarks at the same time.

### Task 1 and Task 2: Analytical Query Library

The six queries are stored as version-controlled Spark SQL files under
`src/data_analysis/sql/` and loaded through one ordered registry.

| Query name | Analysis |
|---|---|
| `monthly_zone_demand` | Monthly taxi demand for each pickup zone |
| `avg_distance_by_weather` | Average trip distance by weather condition |
| `air_quality_demand_relationship` | PM2.5 and hourly taxi-demand relationship |
| `zone_weather_variation` | Zones with the largest weather-related demand variation |
| `peak_hour_by_weekday` | Peak travel hour for each weekday |
| `monthly_demand_trend` | Monthly demand and month-over-month change |

List query names without starting Spark:

```bash
.venv/bin/python -m src.data_analysis.analytical_queries --list
```

Execute all queries and display their results:

```bash
.venv/bin/python -m src.data_analysis queries
```

Execute one query, optionally saving its result as a Delta table:

```bash
.venv/bin/python -m src.data_analysis queries \
  --query monthly_zone_demand --save
```

The normal execution entry uses `integrated_taxi_trips` plus bounded weather
and air-quality views. `--save` writes results under
`data/lakehouse/analysis/query_results/`; it does not perform benchmarking.

#### Default-Configuration Baseline

Generate the six baseline result tables, `EXPLAIN FORMATTED` plans, and timing
statistics using the normal Spark configuration:

```bash
.venv/bin/python -m src.data_analysis.query_benchmark
```

Each query has one warm-up and five measured runs. Outputs are written to:

```text
data/lakehouse/analysis/baseline_queries/
  <query_name>/                 Delta query result
  <query_name>_plan.txt         EXPLAIN FORMATTED plan
  baseline_query_times.csv      Per-run and summary timings
```

This is the original baseline capture. The controlled Task 3 baseline below is
different because it explicitly disables AQE and automatic broadcast joins.

### Task 3: Controlled Query Optimization

The optimization framework keeps the original Task 1/2 SQL files unchanged and
applies experimental variants around them:

- `baseline`: AQE and automatic/adaptive broadcast disabled;
- `cache`: materializes the integrated table with `MEMORY_AND_DISK`;
- `broadcast`: applies explicit broadcast hints to applicable joins;
- `aqe`: enables Adaptive Query Execution;
- `combined`: explicit broadcast plus AQE.

Compare one query with full-table caching:

```bash
.venv/bin/python -m src.data_analysis.optimization_benchmark \
  --query monthly_zone_demand --variant cache
```

Compare all six queries across all variants:

```bash
.venv/bin/python -m src.data_analysis.optimization_benchmark \
  --variant cache broadcast aqe combined
```

The baseline is always added automatically. Every run pins the input Delta
versions, records the effective Spark configuration, uses one warm-up and five
measured actions, captures initial and executed plans, extracts runtime scan and
join metrics, and validates every variant against the baseline result. Validation
preserves duplicate rows and checks ordered names/types, exact non-floating
values, and configured floating-point tolerances.

Each invocation creates a unique directory so earlier evidence is preserved:

```text
data/lakehouse/analysis/optimization/<run_id>/
  environment.json
  status.json
  timings.csv
  benchmark_results.csv
  validation_results.csv
  references/<query>.json
  <variant>/
    configuration.json
    <query>.sql
    plans/<query>_initial.txt
    plans/<query>_final.txt
    metrics/<query>_run_N.json
    results/<query>.json
```

Cache construction time and measured memory/disk placement are reported
separately from query latency. Measurements use warmed access because the OS
file cache is not cleared; a speedup below `1.0` is a valid measured slowdown.

### Task 4: Reusable Analytical Data Products

Task 4 refreshes four Delta products from one pinned version of the integrated
table. It is not part of `python -m src.pipeline all`.

```bash
.venv/bin/python -m src.data_analysis products
```

Refresh only one product:

```bash
.venv/bin/python -m src.data_analysis products \
  --product taxi_zone_statistics
```

| Product | Grain | Intended use | Schema version |
|---|---|---|---:|
| `daily_mobility_summary` | pickup date | Daily operational reporting | 1 |
| `taxi_zone_statistics` | pickup month and zone | Zone planning and taxi operations | 1 |
| `weather_impact_summary` | pickup month and named weather condition | Weather-impact comparisons | 2 |
| `air_quality_impact_summary` | pickup month and PM2.5 band | Environmental mobility analysis | 1 |

The outputs are stored under `data/lakehouse/analysis/products/`. They are
deliberately unpartitioned because the current aggregated products are small;
partitioning would mainly create tiny files and metadata overhead.

Every row includes the product schema version, pinned source table and Delta
version, source schema-version snapshot, creation time, and refresh time. The
`analysis/product_catalog` Delta table additionally records intended users,
grain, materialization rationale, row count, active storage bytes, refresh
duration, and partition strategy. Refreshing one product preserves catalog
records for the others.

`weather_impact_summary` retains the source `weather_condition_code` and adds
the Meteostat `weather_condition` name, such as `Clear`, `Overcast`, or
`Light Rain`. The shared mapping is implemented in
`src/data_analysis/weather_conditions.py`. PM2.5 categories are stable
project-defined analytical bands, not health advice.

Preview products and metadata:

```bash
.venv/bin/python -m src.view_table \
  analysis/products/weather_impact_summary --limit 20
.venv/bin/python -m src.view_table analysis/product_catalog --limit 10
```

#### Standalone HTML Report

Generate the browser report from the four materialized products:

```bash
.venv/bin/python -m src.data_analysis report
```

The default output is `data/reports/task4_data_products_report.html`. It embeds
the compact report data, CSS, and JavaScript, so it can be opened directly in a
browser without Spark, a web server, or external JavaScript dependencies. Use
`--output <path>.html` to select another destination.

### Task 5: Reproduce and Evaluate the Experiments

Run the complete controlled experiment suite serially:

```bash
.venv/bin/python -m src.data_analysis.optimization_suite
```

The suite performs:

1. Three seeded, randomized-order blocks across all six queries and five variants.
2. Partition-pruning experiments with equivalent January time predicates.
3. AQE off/on sensitivity tests with the same 32 initial shuffle partitions.
4. Candidate selection requiring at least 5% median improvement and a majority
   of block wins, followed by three independent confirmation blocks.

Each block uses one warm-up and five measured runs. Inputs are pinned, partition
columns are checked against timestamps before pruning comparisons, and every
optimized result is compared with its baseline. The suite records cache cost,
execution time, speedup, plans, runtime metrics, result equality, and experimental
order. It does not claim statistical significance from three blocks.

Primary outputs are written under:

```text
data/lakehouse/analysis/optimization/suites/<suite_id>/
  aggregate_results.csv
  final_query_comparison.csv
  all_timings.csv
  all_validations.csv
  plan_evidence.json
  suite_manifest.json
  benchmark_report.md
```

A smaller Git-visible evidence bundle is exported to
`docs/benchmarks/week2/<suite_id>/`. Use `--blocks 1` only for a smoke run;
`--output-root` and `--export-root` override the two destinations.

The optimization suite evaluates query execution and does not rebuild the Task 4
products. Product row counts, active storage bytes, and refresh durations are
measured separately in `analysis/product_catalog`; the HTML report consumes the
finished products but does not constitute a query-performance benchmark.

### Verification

Run all unit and Spark integration tests:

```bash
.venv/bin/pytest -q
```

The tests cover configuration contracts, schema-version lineage, strict source
typing and rejected rows, Normal/Integrated transformations, analytical SQL,
optimization result equivalence, product metadata, weather semantics, and
standalone report generation.
