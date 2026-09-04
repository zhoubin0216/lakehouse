# Week 1 Report Notes

Use this file as the shared source for the final 3-5 page design report.

## Data Catalog

- Taxi trips: large fact table, one row per taxi trip.
- Weather: hourly observation table.
- Air quality: hourly sensor measurement table.
- Taxi zone lookup: lookup/dimension table.

## Storage Architecture

- Raw: source files converted to Delta tables.
- Normal: standardized and quality-checked Delta tables.
- Integrated: joined analysis-ready Delta tables.

## Common Data Model

- Lowercase snake_case column names.
- Standard timestamp columns: `pickup_timestamp`, `dropoff_timestamp`, `event_timestamp`, `event_hour`.
- Standard date partition columns: `pickup_year`, `pickup_month`, `pickup_date`, `event_year`, `event_month`.

## Benchmark

Compare two taxi trip storage strategies:

- Monthly partitioning: `pickup_year`, `pickup_month`.
- Daily partitioning: `pickup_date`.
