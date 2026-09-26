# Week 3 Release 2 — Report Evidence Sheet

This sheet records the completed local `week3_release2` execution on 26 September
2026. It is intended as source material for the Week 3 design and evaluation
reports. The immutable manifest, durable release state, verification output,
monitoring Delta tables, and validation summary remain the authoritative
machine-readable evidence.

## Update contents and results

| Dataset | Input records | Inserted raw | Duplicates | Cleaning rejects | Normal changes | Schema |
|---|---:|---:|---:|---:|---:|---|
| Yellow taxi trips | 716,607 | 573,286 | 143,321 | 5 | 573,281 | v1 |
| Weather hourly | 168 | 168 | 0 | 0 | 168 | v1 → v2, added `humidity` |
| Air quality | 1,008 | 1,008 | 0 | 0 | 168 hourly groups | v1 → v2, added `aqi` |
| Taxi zone lookup | 0 | 0 | 0 | 0 | 0 | v1, unchanged |

The taxi update represents 6% new trips and 1.5% duplicates relative to the
9,554,778-row original raw table. The five semantically invalid trips exceeded
the configured distance range. They were retained in quarantine and excluded
from Normal, Integrated, and analytical products without interrupting the
release.

## Timings

| Measurement | Seconds |
|---|---:|
| End-to-end incremental release | 187.8235 |
| Yellow taxi dataset processing | 107.0549 |
| Weather dataset processing | 6.4245 |
| Air-quality dataset processing | 24.6305 |
| Unchanged taxi-zone processing | 2.2803 |
| Integrated-table refresh | 15.6127 |
| Aggregate refresh | 11.6685 |
| Daily mobility product refresh | 2.8741 |
| Taxi-zone statistics refresh | 2.6491 |
| Weather-impact product refresh | 2.0898 |
| Air-quality-impact product refresh | 3.0561 |
| Aggregate plus product refresh | 22.3377 |

Dataset times overlap with later pipeline phases only at the report level; they
must not be added to the end-to-end time as if they were independent runs.

## Validation and monitoring evidence

The generated `validation/rule_summary` contains:

| Rule | Stage | Failed records |
|---|---|---:|
| `duplicate.existing_record` | deduplication | 143,321 |
| taxi distance outside allowed range | cleaning | 5 |

Monitoring recorded one successful release and four successful dataset updates.
The slowest dataset was Yellow Taxi at 107.05 seconds. Schema history contains
two compatible `ADD_COLUMN` events: `humidity` for weather and `aqi` for air
quality. The monitoring validation-failure count is 143,326: 143,321 duplicate
rule failures plus five cleaning failures. Exact duplicates are reported in the
dedicated duplicate metric, while the five cleaning rejects contribute to the
general rejected-record count.

## Analytical consistency

After the release:

- the Integrated table contains 10,124,668 distinct trips;
- 573,281 new Integrated trips have non-null `humidity` and `aqi`;
- all four affected product scopes match a full recomputation;
- all historical product rows outside January 2025 remain unchanged;
- all six existing Week 2 analytical queries execute successfully;
- replaying the completed manifest produces zero Delta writes.

| Product | Total rows | Affected rows verified |
|---|---:|---:|
| Daily mobility summary | 98 | 7 |
| Taxi-zone statistics | 1,016 | 243 |
| Weather-impact summary | 43 | 10 |
| Air-quality-impact summary | 8 | 2 |

## Production-evaluation evidence

The controlled 10,000-row Task 5 experiment, rerun after the real release,
measured:

| Measurement | Result |
|---|---:|
| Synthetic duplicate-aware update median | 2.5711 s |
| Four-product scoped refresh median | 2.8496 s |
| Validation overhead | 0.0296 s |
| Monitoring Delta-append overhead | 0.5031 s |
| Core-table physical storage | 6,210,694,787 bytes |
| Supporting storage | 248,400,652 bytes |
| Supporting storage relative to core | 4.000% |

The monitoring percentage appears large because its comparison workload is very
short; the report should emphasize the approximately 0.50-second absolute cost
of a small Delta commit. The 4% storage overhead is dominated by the 228 MB of
durable release staging/checkpoint data. This state is intentionally retained
until verification and report preparation are complete, because it supports
recovery and auditability.

## Evidence locations

- Manifest: `data/raw/updates/week3_release2/manifest.json`
- Durable state: `data/metadata/updates/week3_release2/state.json`
- Verification: `data/metadata/updates/week3_release2/verification.json`
- Monitoring: `data/lakehouse/monitoring/`
- Validation summary: `data/lakehouse/validation/rule_summary/`
- Quarantine: `data/lakehouse/rejected/`
- Task 5 measured report: `docs/benchmarks/week3/production_readiness.md`
- Latest isolated evaluation artifacts:
  `data/evaluation/week3/20260926T081218Z_7bd4b384/`
