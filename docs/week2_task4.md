# Week 2 Task 4: Reusable Analytical Data Products

## Architecture

Task 4 is implemented as an independent analytical workload. It does not run
inside the recurring Raw -> Normal -> Integrated -> Aggregate pipeline.

```text
Integrated Delta snapshot
        |
        +--> Daily Mobility Summary
        +--> Taxi Zone Statistics
        +--> Weather Impact Summary
        +--> Air Quality Impact Summary
        |
        +--> Product Catalog
```

All four products are generated from one pinned version of
`integrated/integrated_taxi_trips`, preventing one refresh from mixing source
snapshots. A complete refresh is started with:

```bash
.venv/bin/python -m src.data_analysis products
```

## Product Contracts

### Daily Mobility Summary

- Grain: one row per pickup date.
- Users: municipal mobility planners and operations teams.
- Usefulness: provides daily demand, passenger volume, distance, duration,
  revenue, and deterministic peak-hour metrics.
- Why materialized: daily reports otherwise require a full trip-table scan and
  repeated peak-hour window aggregation.

### Taxi Zone Statistics

- Grain: one row per pickup month and pickup taxi zone.
- Users: transport planners and taxi operators.
- Usefulness: supports comparisons of demand and trip characteristics across
  zones and boroughs.
- Why materialized: zone-level aggregation has relatively high cardinality and
  is reused by planning reports.

### Weather Impact Summary

- Grain: one row per pickup month and named weather condition.
- Users: mobility operations and emergency planning teams.
- Usefulness: exposes changes in demand, distance, duration, and fares under
  different observed weather conditions.
- Why materialized: avoids repeatedly grouping millions of enriched trips by
  weather condition.
- Semantics: retains the source Meteostat `weather_condition_code` and adds its
  `weather_condition` label. The mapping is shared code rather than
  presentation-only text.

### Air Quality Impact Summary

- Grain: one row per pickup month and project-defined PM2.5 category.
- Users: environmental and transport policy analysts.
- Usefulness: provides a stable categorization for comparing mobility metrics
  across PM2.5 concentration bands.
- Why materialized: centralizes category rules and avoids repeated
  classification and aggregation. The bands are analytical project categories,
  not health advice.

## Metadata and Refresh Semantics

Every product row includes:

- `_product_name`
- `_product_schema_version`
- `_source_table`
- `_source_delta_version`
- `_source_schema_versions`
- `_created_at`
- `_refreshed_at`

The Delta catalog at `analysis/product_catalog` also stores the product grain,
intended users, description, materialization reason, row count, active storage
bytes, refresh duration, and partition strategy. The first creation time is
preserved across refreshes; the refresh time and measurements are replaced.

Products use overwrite refreshes so a failed source-to-product interpretation
cannot silently mix old and new product rows. Catalog replacement happens only
after every selected product write succeeds. Product writes and the final
catalog write are separate Delta transactions, not one cross-table transaction.

## Physical Layout

The four outputs are deliberately unpartitioned. Their row counts are small
because they are already aggregated by day, zone/month, or condition/month.
Partitioning these tables at the current scale would create small files and
increase metadata overhead. Partitioning should be reconsidered only if a
product grows enough that common selective predicates can skip substantial
data while maintaining reasonably sized files.

## Standalone Visual Report

The reusable report generator reads the four analytical products, builds a
compact browser payload, and writes a dependency-free HTML document:

```bash
.venv/bin/python -m src.data_analysis report
```

The default file is `data/reports/task4_data_products_report.html`. It opens
directly in a browser and does not require Spark after generation.
`src/data_analysis/data_product_report.py` separates product extraction,
payload construction, HTML rendering, and file output so scheduled jobs or
other Python entry points can reuse the same implementation.
