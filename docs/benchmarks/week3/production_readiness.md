# Week 3 Task 5 — Production Readiness Evaluation

## Scope and method

This run used Spark 3.5.9 on `local[4]` with 10,000 synthetic base records and 3 measured repetitions. Timings are local, warmed-access wall-clock measurements; the operating-system cache was not cleared. The suite writes only below its unique evaluation directory.

The synthetic experiment makes component overhead reproducible and avoids rewriting the municipality tables. If a completed real release state is available, its separately instrumented timings are shown below and should be preferred for workload conclusions.

## Results

| Measurement | Result | Scope |
|---|---:|---|
| Incremental update median | 2.5711 s | 1,100 incoming; 1,000 inserted; 100 quarantined duplicates |
| Analytical refresh median | 2.8496 s | Four products over 5,000 affected rows |
| Validation overhead | 0.0296 s (96.4%) | Three rules over 10,000 rows |
| Monitoring overhead | 0.5031 s (2768.6%) | One Delta metadata append per operation |
| Latest real release update | 187.8235 s | Release `week3_release2`; status `complete` |
| Latest real analytical refresh | 22.3377 s | Aggregate plus affected-product refresh |

Per-product scoped refresh medians:

| Product | Median seconds | Output rows |
|---|---:|---:|
| `daily_mobility_summary` | 0.8330 | 1 |
| `taxi_zone_statistics` | 0.6785 | 10 |
| `weather_impact_summary` | 0.6737 | 5 |
| `air_quality_impact_summary` | 0.6516 | 3 |

## Storage overhead

Physical bytes include active data, retained Delta history, and transaction logs. This is a filesystem snapshot, not only active Parquet bytes.

| Category | Bytes | Files |
|---|---:|---:|
| core tables | 6,210,694,787 | 596 |
| analytical products | 746,673 | 152 |
| validation | 13,923,284 | 64 |
| monitoring | 199,648 | 58 |
| metadata and update state | 233,531,047 | 117 |

Supporting tables, products, and metadata occupy **248,400,652 bytes**, or **4.000%** of the configured core-table footprint at measurement time.

## Production-readiness assessment

Week 1 choices that simplified maintenance were immutable raw data, layered tables, central YAML contracts, shared Delta I/O helpers, and source lineage. They allowed incremental processing, monitoring, and validation to be added without rewriting the analytical SQL contract.

The largest modifications were the release state machine and dependency-aware refresh. They must coordinate idempotent writes, schema versions, affected scopes, retries, and cross-layer consistency. Validation was smaller because accepted/rejected splitting was already present and could be generalized into rule objects.

Future datasets are supported well when they can declare a source schema, business key, cleaner, and dependency mapping. The remaining coupling is the fixed dictionaries for raw/normal keys and product dependencies. A redesign would move those declarations into a typed dataset registry, use an external transaction/orchestration service for multi-table commits, and export metrics to a time-series alerting system rather than relying only on local Delta queries.

## Interpretation and limitations

Validation cost depends on rule complexity and rejection rate; reference joins should be measured separately when lookup tables stop fitting in broadcast memory. Monitoring cost is dominated by small Delta commits, so batching records would reduce overhead at higher run frequency. Local synthetic timings demonstrate the method and relative component cost, not cluster-scale throughput or statistical significance. Rerun on the deployment hardware and retain multiple real releases before setting service-level objectives.

Artifacts: `measurements.json`, `measurements.csv`, and the isolated `artifacts/` Delta tables. All counts are checked across repetitions before a result is reported.
