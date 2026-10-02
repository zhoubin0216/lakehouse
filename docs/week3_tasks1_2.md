# Week 3 Tasks 1-2

## Release Contract

`src.incremental.generate` reads the original schema-v1 raw snapshot and writes
four files to `data/raw/updates/week3_release2/`. It never modifies the original inputs.
`manifest.json` records the actual counts and SHA-256 checksums. Both taxi
percentages use the original raw row count as denominator: 6% newly timestamped
trips and 1.5% exact original copies. Distances, fares, vendors and locations stay
correlated by sampling whole valid original rows. Timestamps follow the maximum
original pickup; durations are preserved. The weather and air updates cover
168 consecutive local hours after the original coverage ends. Air contains
observations for the original NYC monitoring-site/POC combinations. This is a
NYC-scoped update to the national input, not a claim of complete national coverage.
The static zone dimension has no changes and its CSV therefore has only a header.

The weather `humidity` field is an additive numeric percentage. The original
`rhum` -> `relative_humidity_pct` mapping stays unchanged for query compatibility.
`aqi` is an illustrative synthetic indicator, not a regulatory AQI conversion.
The hourly normal air table uses the maximum supplied station AQI, while PM2.5
mean/min/max and existing product category definitions retain their old semantics.
Both new columns reach integrated; old rows expose null, never fabricated values.

The existing local raw tables predate strict type contracts: some weather numeric
fields were inferred as integer/string, and air time-only fields as timestamps.
`fit_legacy_raw` preserves those physical types without rebuilding the table,
after validating the release against its declared contract. It permits only
lossless numeric adaptation and stores the original contract values in
`_source_payload` when adaptation is needed. Numeric codes retain their exact
source spelling in this payload. Legacy time-only timestamps use an irrelevant
fixed winter date; the actual event date always comes from `date_local`/`date_gmt`.
A fractional update to an integer-only legacy column stops with an explicit
migration-required error instead of truncating. Fresh tables use configured types.

## Update Path

```text
manifest + update files
  -> strict version-specific parsing / consumption rejects
  -> compare content with existing raw records / append new revisions
  -> clean affected keys or full affected air-quality hours / cleaning rejects
  -> MERGE changed normal keys
  -> rejoin new trips and old trips affected by changed context/zone keys
  -> MERGE changed integrated trip IDs
  -> replace affected aggregate groups
  -> replace affected date/month scopes of dependent analytical products
```

Raw deduplication uses null-preserving JSON of business fields, not filenames,
ingestion timestamps or only an in-batch `dropDuplicates`. Weather, zones and air
use stable business keys to find the latest revision; reverting to an earlier
value is a legitimate change. The latest arrival takes precedence over schema
version because schema version is not a record modification time. Conflicting
revisions of one key inside the same release are rejected at the release level.

The source taxi dataset has no stable trip identifier. Exact row identity remains
the existing `_record_hash`/`trip_id` convention, preserving Week 2 IDs. Updates
to a taxi fare cannot be safely linked to the original trip without a producer
trip ID or an explicit correction mapping. Such modifications are treated as new
trips; the generated taxi release contains only inserts and exact duplicates.
Weather, air and zone corrections are true keyed updates.

Each release stores durable Delta staging tables, pinned starting versions and
stage checkpoints. Raw/rejected appends have stable Delta transaction IDs;
normal/integrated MERGEs and scoped group replacement are replay-safe. If a
downstream stage fails after raw succeeds, rerunning the same manifest continues
the incomplete stages rather than skipping them because no new raw rows remain.
Completed manifests perform no writes. Files/configuration cannot mutate halfway
through a release; a different pending release blocks newer releases.

This is a local single-writer protocol, not a distributed transaction across all
tables. During a failed/in-progress run, direct readers may see tables at different
stages. Consume published products after the release state is `complete`; for
strict concurrent publication, add a release-to-table-version pointer and require
readers to pin that published bundle. Manual full rebuilds must not run concurrently.

## Task 2 Decisions

| Product | Refresh scope | Why |
| --- | --- | --- |
| daily_mobility_summary | affected pickup dates | Recompute sums/averages and peak hour from complete dates. |
| taxi_zone_statistics | affected pickup months | Preserve exact averages and remove obsolete zone groups. |
| weather_impact_summary | affected pickup months | Weather corrections may move trips between categories. |
| air_quality_impact_summary | affected pickup months | PM2.5 corrections may move trips between categories. |
| avg_trip_duration_per_day | affected dates | Complete-group mean, not a mean of means. |
| trips_per_borough / avg_fare_per_borough | affected boroughs across history | These existing products have no time grain; a borough's full group is needed. |

Product dependencies include their business columns and displayed schema-lineage
columns. For example, changing only `humidity` within schema v2 does not refresh
any existing product because none uses that measure; a weather condition change
refreshes the weather product, while a source-version change also affects the
lineage displayed by the other products. Products keep metadata on untouched rows;
`_source_delta_version` and `_source_schema_versions` describe each refreshed scope.
The catalog stores total row count/storage and the last refresh's source metadata.

Delta `replaceWhere` atomically replaces complete scopes, including groups whose
last row has disappeared from that category. It does not replace the whole table.
Integrated month partitions make these reads prunable. Average/peak-hour results
remain equivalent to full recomputation; no unsupported subtraction of averages
is used. Late weather/air/zone corrections re-enrich matching historical trips.

For these products, ordinary inserts and keyed context corrections need no full
product rebuild. Full recomputation is appropriate for changed category definitions,
grain, business logic, incompatible type/unit/timezone changes, historical backfills
covering all groups, or missing initial product tables. Cross-history borough
aggregates can still touch most trips; future sufficient-statistic tables
(sum/count plus careful correction deltas) would reduce that cost further.

Approved additive nullable columns evolve automatically in Delta. Source schemas
remain explicit and strict: an unregistered column is not silently accepted.
Register a new version, map/validate it and route the release to it. Renames,
narrowing type conversions, changed units, keys or semantics require a reviewed
migration/backfill. Existing version definitions must remain readable.

The original taxi coverage is Q1 2024; the simulated overlapping update period is
January 1-7, 2025 because original weather and air cover all 2024. Quality rules
list these two periods explicitly. Analytical/optimization context views exclude
the unobserved April-December gap instead of treating it as zero taxi demand.

## Verification

Run `pytest tests/test_incremental.py` for record-level deduplication, correction
and reversion behavior, schema evolution, dependency filtering, stale-group deletion,
and incremental-vs-full product equivalence. Run `python -m src.pipeline updates`
twice to check real release replay. Actual release counts/checkpoints are in
`data/metadata/updates/week3_release2/state.json`; generated file counts are in its manifest.
Staging is retained to support replay/audit. A retention/cleanup policy and Delta
monitoring tables belong to the subsequent operations tasks.

`python -m src.incremental.verify` also checks the real release: raw count deltas,
duplicate counts, new-column availability, unique integrated trip IDs, affected
product results against recomputation, unchanged historical product rows, six
existing SQL queries, and zero Delta commits when replaying a completed release.
The resulting evidence is written to the release's `verification.json`.

## Generated Release

Release `week3_release2` covers January 1-7, 2025. Both consumption and cleaning
reported zero rejected rows for this final release.

| File | Input rows | New raw rows | Ignored duplicates | Schema addition |
| --- | ---: | ---: | ---: | --- |
| taxi_updates.parquet | 716,607 | 573,286 | 143,321 | none, version 1 |
| weather_updates.csv | 168 | 168 | 0 | humidity, version 2 |
| air_quality_updates.csv | 1,008 | 1,008 | 0 | aqi, version 2 |
| taxi_zone_updates.csv | 0 | 0 | 0 | unchanged, version 1 |

The 1,008 station observations produce 168 new hourly air-quality rows. Integrated
receives 573,286 new trips. Product row counts after refresh are 98 daily summaries,
1,016 monthly zone summaries, 43 monthly weather summaries and 8 monthly PM2.5
category summaries. Existing product scopes remain in place.

Real-data verification passed: integrated contains 10,124,673 unique trip IDs;
all 573,286 new trips have both humidity and AQI. All four products match scoped
recomputation, all historical product rows (including metadata) are unchanged,
all six existing SQL queries execute, and completed-release replay creates zero
Delta commits. Evidence: `data/metadata/updates/week3_release2/verification.json`.

During development, an earlier generator trial was restored to its verified
starting snapshots and archived under `data/raw/update_validation_attempts/` and
`data/metadata/validation_attempts/`. It is not an active release. Delta history
retains the trial and restore commits for audit; no original input was deleted.
