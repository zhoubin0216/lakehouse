# Week 2 Optimization Design — Task 3 and pre-product Task 5

## Responsibility and input contract

Task 1/2 own analytical definitions and the original six SQL files under
`src/data_analysis/sql/`. This work adds equivalent implementations and controlled
experiments; it does not edit those definitions or construct Task 4 products.
Weather-code labels and the interpretation of Q4 remain subject to their owner's
confirmation. Re-run the suite if that contract changes.

The workload uses the integrated trip fact table plus hourly weather/air-quality
Delta tables. All three Delta versions are pinned at the start of a suite. Metadata
uses active snapshot files/bytes rather than physical directory totals, which may
include superseded Delta versions. Local wall-clock timestamps stay `timestamp_ntz`.

## Experimental design

Baseline is a controlled configuration, not an attempt to disable every Spark
optimization: it disables AQE and automatic/adaptive broadcast and clears table
caches, while retaining Delta statistics, vectorized scans and column pruning.
Each configuration has three blocks, each with one warm-up and five timed actions.
Variant order is shuffled with a recorded seed. Correctness references, cache
materialization, validation and plan/metric export are outside query timing.
An untimed correctness reference warms file access before each block. OS cache is
not cleared; measurements must not be described as cold-disk tests.

Exploration measures all six original questions under baseline, full-table cache,
explicit broadcast, AQE-only, and broadcast+AQE. A candidate requires at least a
5% pooled-median improvement and wins in a majority of exploratory blocks. Hints
that do not apply to a query cannot be selected. Selection is frozen before a
separate three-block confirmation, which reports regressions rather than hiding
them. If no candidate qualifies, retain baseline and state that no improvement
was established. Three blocks do not establish statistical significance.

## Optimization decisions and trade-offs

- **Cache:** materialize the full integrated view using `MEMORY_AND_DISK`, once
  per configuration. Its build cost is shared by the six-query workload, not
  included in latency or charged six times. Record actual RDD memory/disk bytes
  and cached partitions. Cache latency improvements alone do not establish
  amortization; wider cached representations may lose to warmed column-pruned
  Parquet scans. A negative result is retained.
- **Partition pruning:** run Q1 for one month under identical function-based
  and range-based time predicates. Add equivalent year/month partition predicates
  only in the optimized arm, after checking complete-data partition/timestamp
  consistency. The range case is a control for Delta data skipping. Do not compare
  a full-quarter answer with a January answer. No duplicate fact table is needed.
- **Broadcast:** keep automatic/adaptive broadcasting disabled in both arms.
  Q4 explicitly broadcasts hourly weather for the large fact join and the small
  zone/weather-demand aggregate for the outer join. Q3 broadcasts its hourly
  demand aggregate, not the preserved left side of its left join. Q6 broadcasts
  the three-row previous-month aggregate. Q1/Q2/Q5 have no relevant join and their
  broadcast timings are no-op controls. Replication cost/memory constrain use at
  larger scales; hints should be revisited when input sizes change.
- **AQE:** change only `spark.sql.adaptive.enabled`; keep adaptive broadcast
  disabled to separate its effect from explicit hints. Main tests use four
  initial shuffle partitions. A separate Q1/Q4 sensitivity experiment uses 32
  in both arms. Compare final coalesced shuffle reads and partition metrics;
  do not claim skew optimization or dynamic join conversion without evidence.
- **Combined:** explicit broadcast hints plus AQE, without caching. It is a
  separate treatment; its time is not presented as the isolated effect of either
  technique.

## Correctness, physical plans and runtime evidence

Every measured result is compared against the untimed baseline and its variant's
first result. Ordered column names/types must match; nullability/metadata may
change. Comparison ignores row order, preserves duplicates, checks non-floating
values exactly, and applies declared tolerances only to top-level float/double
columns. Exact SHA-256 hashes and tolerance decisions are separately recorded.
A mismatch stops the job and retains its diagnostics.

Plans include `EXPLAIN FORMATTED` before execution and a formatted plan from the
same DataFrame after `collect()`, so AQE's final plan is available. Runtime SQL
metrics traverse adaptive/query-stage wrappers and deduplicate shared plan nodes.
Selected-file bytes are not actual physical bytes read after column pruning;
do not use them as an I/O bandwidth estimate. Cache storage evidence includes
internal Delta-state RDDs, which must not be mistaken for a cached fact table.

## Evaluation and remaining integration

The suite generates measured before/after tables, an independent six-query
confirmation summary, validation logs, physical-plan evidence and a benchmark
report. It exports a compact unique bundle under `docs/benchmarks/week2/`, while
complete artifacts remain under the ignored local `data/` directory. No source
data, historical versions or original baseline results are overwritten. No
commit or push is performed automatically.

**Pending Task 4:** product table paths/schemas, partition layout, source/creation/
refresh/schema metadata, active storage overhead, refresh cost and equivalent
on-demand-vs-product query comparison. Task 5 remains pre-product until these
measurements are added. Product build work remains with its owner.

For ten cities, recommendations are conditional on these one-city measurements:
add city/time contracts, choose city/date layouts from measured file sizes,
broadcast only verified-small dimensions, budget decoded cache memory, retest AQE
and shuffle counts on the larger cluster, instrument skew/spill, and use incremental
product refresh. No ten-city performance result is implied by this experiment.

## Commands

```bash
.venv/bin/pytest -q
.venv/bin/python -m src.data_analysis.optimization_suite
```

Run tests before or after benchmarks, never concurrently. The generated report
and evidence bundle identify the exact input versions, implementation hashes,
seed, settings and measured outcomes.
