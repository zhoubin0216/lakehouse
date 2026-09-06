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

## Data Consumption

- Use source file registry instead of only recording consumed file names.
- Track each source file by path, size, modification time, optional checksum, and ingestion status.
- Add lineage columns to raw tables: `_source_file`, `_source_file_size`, `_source_modified_time`, `_ingestion_run_id`, `_ingestion_timestamp`, `_record_hash`.
- Consume only new or changed files.
- Write raw Delta data first, then update registry and ingestion metadata after the write succeeds.
- Deduplicate raw records using `_record_hash` or dataset business keys to reduce duplicate ingestion after retries.

## Common Data Model

- Lowercase snake_case column names.
- Standard timestamp columns: `pickup_timestamp`, `dropoff_timestamp`, `event_timestamp`, `event_hour`.
- Standard date partition columns: `pickup_year`, `pickup_month`, `pickup_date`, `event_year`, `event_month`.

## Benchmark

Compare two taxi trip storage strategies:

- Monthly partitioning: `pickup_year`, `pickup_month`.
- Daily partitioning: `pickup_date`.
