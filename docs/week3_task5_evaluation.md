# Week 3 Task 5 — Production-Readiness Evaluation

## Evaluation goals

Task 5 evaluates whether the platform can be operated and extended, rather than
only whether a single pipeline run succeeds. The implementation measures all
five required areas:

1. duplicate-aware incremental update time;
2. affected analytical-product refresh time;
3. physical storage overhead;
4. validation overhead;
5. monitoring overhead.

The command `python -m src.evaluation` produces machine-readable JSON/CSV,
isolated Delta evidence, and a short Markdown report. Defaults are controlled by
the `evaluation` section in `configs/config.yaml`.

## Measurement strategy

### Incremental update

The controlled fixture writes a baseline raw Delta table, creates a 10% update
plus 1% duplicates, runs the production `split_novel_records` logic, verifies
accepted/rejected counts, and writes the accepted rows to Delta. The timing
therefore includes duplicate detection, comparison with existing content,
materialization, and the raw insertion. Downstream recomputation is measured
separately so its cost is not hidden inside the ingestion number.

For real releases, `last_attempt_seconds` remains the end-to-end update time in
the durable release state and monitoring table.

### Analytical refresh

The fixture creates an integrated table with two months of records, selects one
affected month, and refreshes the same four `PRODUCT_BUILDERS` used by the
platform. It records total and per-product times and verifies stable output row
counts across repetitions.

The real incremental pipeline now records:

- `integration_refresh_seconds`;
- `aggregate_refresh_seconds`;
- `product_refresh_seconds` by product;
- `analytical_refresh_seconds` as aggregate plus product refresh.

These fields are checkpointed with the release state, so retry behavior and
measurement evidence remain auditable.

### Validation overhead

A cached input is processed through the same accepted/rejected split twice:
once with no rules and once with required-field, non-empty-text, and range
rules. Median elapsed time is compared. Rejected rows and the rule summary are
written under the suite's isolated lakehouse root to verify the complete
quarantine/report path.

This is a representative cost, not a universal constant. Complex regular
expressions, Python UDFs, many rules, high rejection rates, or large reference
joins can change the result substantially.

### Monitoring overhead

Both arms execute the same cached Spark aggregation. The enabled arm then
appends one operation record to the production monitoring Delta schema; the
disabled arm calls the same API with monitoring disabled. This isolates the
small-commit cost while retaining a realistic business action in both arms.

Because a Delta transaction has a relatively fixed cost, monitoring overhead is
usually more significant for tiny, frequent operations than for long-running
updates. A higher-throughput deployment should batch events or send them to a
dedicated metrics system.

### Storage overhead

The report measures physical files, including Parquet data, Delta logs, and
retained history. It separates:

- core raw, normal, integrated, and aggregate tables;
- analytical products and their catalog;
- validation quarantine and summary;
- monitoring tables;
- metadata and incremental state.

This complements active-file sizes from Delta metadata. Physical size is the
relevant local-disk cost until a retention policy safely removes obsolete
history.

## Maintainability analysis

### Week 1 decisions that helped

Immutable raw records and explicit lineage made updates auditable. Layered raw,
normal, integrated, and aggregate tables provided clear places for validation
and partial refresh. Central YAML schema contracts made additive versions
possible without editing every reader. Shared Spark/Delta helpers kept storage
behavior consistent. The accepted/rejected split provided a natural base for
Task 4's rule objects.

### Components requiring the largest changes

The incremental release state machine required the most work. It coordinates
checksums, retries, idempotent Delta transactions, schema versions, affected
partitions, and dependency-aware product refresh. Cross-layer consistency is
harder than an initial full build because a failure may occur after some tables
have committed.

Monitoring was structurally smaller but touched orchestration boundaries so
that successful, failed, skipped, and resumed attempts retain correct counts.
Validation required changes in every layer to guarantee that bad rows are not
reintroduced after cleaning or reference joins.

### Support for future datasets

The platform is extensible for a new dataset that can provide:

- versioned source schemas and column mappings;
- raw and normal table paths;
- a stable business key;
- a cleaner and validation rules;
- dependency information for integrations and products.

Rule classification, quarantine storage, validation reporting, monitoring, and
evaluation can then be reused. The main limitation is that dataset keys,
cleaners, and product dependencies still use code dictionaries.

### What should be redesigned

A next version should use one typed dataset registry for schemas, keys,
cleaners, validation, and dependencies. A production orchestrator or table
format supporting multi-table transactions should replace the local JSON state
and single-writer lock. Metrics should be exported to a time-series backend with
alerts, while Delta retains detailed audit records. Reference validation should
choose broadcast, partitioned, or indexed strategies from table statistics.
Finally, evaluation should run automatically in CI on a small fixture and on a
scheduled representative-scale environment so regressions are visible over
time.

## Interpreting results

Local results support engineering comparisons on the same machine; they must
not be extrapolated directly to a cluster. At least three measured repetitions
are recommended, with the median reported. The operating-system cache is not
cleared, so the suite explicitly describes results as warmed-access timings.
Before setting service-level objectives, collect multiple real releases,
measure peak data volumes and concurrent queries, and rerun the suite on the
deployment hardware.
