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
- Track each source file by path, size, modification time, optional checksum, schema version, and ingestion status.
- Add lineage columns to raw tables: `_schema_version`, `_source_file`, `_source_file_size`, `_source_modified_time`, `_ingestion_run_id`, `_ingestion_timestamp`, `_record_hash`.
- Consume only new or changed files.
- Write raw Delta data first, then update registry and ingestion metadata after the write succeeds.
- Deduplicate raw records using `_record_hash` or dataset business keys to reduce duplicate ingestion after retries.
- Preserve all source contracts under `schema_versions` and select the ingestion contract through `current_schema_version`.
- Treat the selected `_schema_version` as immutable ingestion-time lineage. Moving the current-version pointer alone does not re-consume unchanged files or rewrite old raw rows; historical reinterpretation requires an explicit backfill.
- Define an explicit `column_types` entry for every expected column in every schema version.
- Require exact Parquet physical types; read CSV fields as strings and use safe conversion rather than schema inference.
- Write row-level CSV conversion failures to `rejected/consumption/<dataset>` with original values and `_rejection_reasons`; treat missing or unexpected columns as a failed ingestion run.

## Common Data Model

- Lowercase snake_case column names.
- Standard timestamp columns: `pickup_timestamp`, `dropoff_timestamp`, `event_timestamp`, `event_hour`.
- Standard date partition columns: `pickup_year`, `pickup_month`, `pickup_date`, `event_year`, `event_month`.

## Data Cleaning and Standardization

- For repeated lookup or hourly business keys, prefer the highest source schema version and then the latest ingestion timestamp.
- Preserve a scalar `source_schema_version` for one-record Normal outputs and a sorted `source_schema_versions` set for air-quality hourly aggregates.
- Write records that violate required-field, timestamp, project-period, duration, measurement, or unit rules to `rejected/cleaning/<dataset>` instead of silently dropping them.
- Validate that lookup and hourly-table primary keys are present, non-null, and unique.
- Treat all project event times as local wall-clock values stored as `timestamp_ntz`.
- Construct weather timestamps from the source `year`, `month`, `day`, and `hour` fields.
- Repair air-quality timestamps by combining `date_local` with the time component of `time_local`; the inferred date attached to the raw time value is not reliable.
- Convert weather measurements to explicit numeric types and include units in normalized column names.
- Preserve weather observations when an optional measurement is missing; replace invalid individual measurements with null instead of rejecting the full hourly record.
- Restrict taxi pickups to Q1 2024 and retain trips with durations greater than zero and no longer than 24 hours.
- Use the ingestion `_record_hash` as a stable project-level `trip_id` and deduplicate on that identifier.
- Retain zero-distance trips and negative financial records with `is_zero_distance` and `is_financial_adjustment` flags.
- Replace missing, negative, or implausibly large taxi distances with null and set `has_invalid_distance`.
- Maintain configurable taxi time and distance thresholds in `configs/config.yaml`.

## Air-Quality Transformation

- The source contains PM2.5 observations from across the United States; restrict the contextual dataset to available New York City counties: Bronx, Kings, and Queens.
- Accept non-null, non-negative measurements reported in `Micrograms/cubic meter (LC)`.
- Aggregate valid monitoring-site observations into one city-level PM2.5 value per local hour.
- Retain the hourly average, minimum, maximum, observation count, and distinct monitoring-site count for auditability.

## Integration Strategy

- Preserve source-specific versions as `taxi_schema_version`, pickup/dropoff zone versions, `weather_schema_version`, and `air_quality_schema_versions`; do not assign one ambiguous version to the multi-source row.
- Join taxi trips to the taxi-zone dimension twice to derive pickup and dropoff zone, borough, and service-zone attributes.
- Match weather and air quality to each trip by the local pickup hour.
- Use left joins so missing contextual observations do not remove taxi trips.
- Add `weather_available` and `air_quality_available` flags to make missing context explicit.
- Broadcast the zone, weather, and hourly air-quality tables because they are small relative to the taxi-trip fact table.
- Require hourly weather and air-quality join keys to be unique before integration to prevent row multiplication.

## Validation Results

- Raw taxi trips: 9,554,778 rows.
- Cleaned Q1 2024 taxi trips: 9,551,387 rows.
- Integrated taxi trips: 9,551,387 rows; the integration does not multiply or remove trips.
- Pickup-zone, dropoff-zone, weather, and air-quality coverage are complete for the retained dataset.
- Unit tests cover primary-key validation, weather timestamp construction, air-quality timestamp repair and aggregation, taxi-trip cleaning, and preservation of unmatched trips during integration.

## Integration Limitations

- Hourly matching assumes taxi, weather, and air-quality timestamps use consistent New York local-time semantics.
- A taxi trip receives the observation for its pickup hour rather than an interpolated observation at its exact pickup minute.
- PM2.5 is represented by an hourly city-level average because the provided data has no observations for Manhattan or Staten Island.
- The city-level PM2.5 average improves temporal coverage but does not represent neighborhood-level spatial variation.
- The ingestion record hash is a practical deduplication identifier, not a source-provided taxi-trip primary key.

## Benchmark

Compare two taxi trip storage strategies:

- Monthly partitioning: `pickup_year`, `pickup_month`.
- Daily partitioning: `pickup_date`.
- Preserve source-specific version columns in benchmark tables and include a `schema_versions` JSON snapshot in every benchmark result row.
