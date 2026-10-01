# Week 4 Tasks 1–2: Machine-Learning Dataset and Feature Pipeline

This document is the README-style handoff for the Week 4 machine-learning
work. Task 1 creates the reproducible prediction dataset. Task 2 fits and saves
a reusable Spark ML feature pipeline on top of that stable data contract.

## Selected prediction problem

The project predicts **hourly yellow-taxi pickup demand for each taxi zone**.
One record represents one `pickup_hour` and `pickup_location_id`; the target
column `demand` is the number of accepted trips whose pickup occurred in that
zone during that hour.

This problem matches the municipality's planning use case and uses all four
platform datasets:

- Taxi trips define the demand target.
- Taxi zones provide zone, borough, and service-zone context.
- Weather provides hour-level atmospheric conditions.
- Air quality provides hour-level PM2.5 conditions and observation coverage.

## Generate the Task 1 dataset

The prerequisite is the integrated Delta table produced by the Weeks 1–3
pipeline. From the repository root, run:

```bash
python -m src.machine_learning.training_dataset
```

Use a different configuration file when required:

```bash
python -m src.machine_learning.training_dataset \
  --config configs/config.yaml
```

Then generate the final model-ready dataset and saved feature transformer:

```bash
python -m src.machine_learning.feature_pipeline
```

Install the declared project dependencies before running on a new machine:

```bash
python -m pip install -r requirements.txt
```

The command reads:

```text
data/lakehouse/integrated/integrated_taxi_trips
```

and overwrites the reproducible training dataset at:

```text
data/lakehouse/ml/hourly_taxi_demand
```

The output is partitioned by `split`, so training, validation, and test data
can be loaded independently. Paths and boundaries are controlled by the
`machine_learning` section of `configs/config.yaml`.

## Dataset construction

The builder performs the following steps automatically:

1. Filters the integrated table to the configured observation window.
2. Creates one deterministic weather and air-quality context record per hour.
3. Creates a pickup-zone dimension from valid observed taxi zones.
4. Builds the complete cross product of observed hours and pickup zones.
5. Counts trips for each zone-hour and left-joins those counts to the grid.
6. Fills missing counts with zero so zero-demand examples are preserved.
7. Assigns chronological train, validation, and test splits.
8. Writes the result as a partitioned Delta table.

The full grid is important: a direct `groupBy(...).count()` would omit every
zone-hour with no pickups and bias the training data toward positive demand.

## Data contract

Keys and target:

| Column | Meaning |
|---|---|
| `pickup_hour` | Start of the local prediction hour |
| `pickup_location_id` | Taxi-zone identifier |
| `demand` | Number of accepted pickups in the zone-hour |
| `split` | `train`, `validation`, or `test` |

Location feature inputs:

```text
pickup_zone
pickup_borough
pickup_service_zone
```

Weather feature inputs:

```text
temperature_c
relative_humidity_pct
precipitation_mm
snow_depth_mm
wind_direction_deg
wind_speed_kmh
wind_gust_kmh
pressure_hpa
cloud_cover_pct
weather_condition_code
weather_available
```

Air-quality feature inputs:

```text
pm25_avg_ug_m3
pm25_min_ug_m3
pm25_max_ug_m3
air_quality_observation_count
air_quality_site_count
air_quality_available
```

Trip-level fare, duration, drop-off, payment, and passenger fields are not
included. They are unavailable before the target hour finishes and would not
be valid predictors for advance demand planning.

## Task 2 feature engineering pipeline

The fitted Spark `PipelineModel` applies the same transformations during
training, validation, testing, retraining, and future inference:

```text
SQLTransformer (temporal and availability features)
  -> Imputer (training medians)
  -> VectorAssembler (numerical features)
  -> StandardScaler (numerical features only)
  -> StringIndexer (categorical features)
  -> OneHotEncoder
  -> VectorAssembler (final features vector)
```

The pipeline is fitted only on `split = 'train'`. The fitted medians, standard
deviations, category dictionaries, and vector layout are then reused for the
validation and test rows. `handleInvalid = 'keep'` gives previously unseen
categories an explicit bucket instead of failing or silently dropping records.

### Generated temporal features

| Feature | Rationale |
|---|---|
| `hour_of_day` | Captures commuting, nightlife, and airport demand cycles |
| `day_of_week` | Separates weekday and weekend travel patterns |
| `month` | Captures broad seasonal change and supports longer future datasets |
| `is_weekend` | Provides a direct weekend indicator for models that need it |

`hour_of_day`, `day_of_week`, and `month` are treated as categorical because
their numeric values do not represent ordinary linear distance. `is_weekend`
is kept as a numerical binary feature.

### Location and categorical features

```text
pickup_location_id
pickup_borough
pickup_service_zone
hour_of_day
day_of_week
month
weather_condition_code
```

`pickup_location_id` supplies zone-level resolution, while borough and service
zone give coarser geographical structure. The human-readable `pickup_zone`
name remains available in the Task 1 table but is not encoded alongside the ID,
because both columns represent the same identity.

### Numerical features

```text
temperature_c
relative_humidity_pct
precipitation_mm
wind_direction_deg
wind_speed_kmh
pressure_hpa
cloud_cover_pct
pm25_avg_ug_m3
pm25_min_ug_m3
pm25_max_ug_m3
air_quality_observation_count
air_quality_site_count
is_weekend
weather_available_flag
air_quality_available_flag
```

Numerical nulls are replaced with medians learned from the training split.
Only the numerical vector is standardized; one-hot categorical vectors retain
their binary interpretation. Centering is disabled so the final combined
feature vector can remain sparse.

The real training window showed that `snow_depth_mm` and `wind_gust_kmh` were
entirely null. A median cannot be learned for an all-null column, and constant
zero values would not add information, so these attributes are removed from
the baseline feature configuration. They can be restored by adding their names
to `numerical_features` when future datasets provide usable values.

The high-cardinality taxi-zone field required the most categorical
preprocessing because its dictionary must be learned without exposing future
splits and must still tolerate new zones. The all-null weather fields required
the main feature-selection decision. Taxi trips and taxi zones provide the most
direct demand signal and spatial structure; weather adds plausible short-term
operational context, while air quality is retained as a potentially useful but
more exploratory citywide signal.

### Task 2 outputs

The final Delta table is:

```text
data/lakehouse/ml/hourly_taxi_demand_prepared
```

It intentionally retains only:

```text
pickup_hour
pickup_location_id
demand
split
features
```

Keys and split metadata support evaluation and error analysis; raw and
intermediate attributes are removed from the model-facing table. The fitted
feature transformer is saved at:

```text
data/lakehouse/ml/models/hourly_taxi_demand_features
```

The current configuration produces a **352-element feature vector** and keeps
the same 396,144 train, 87,770 validation, and 88,032 test rows generated by
Task 1.

## Chronological splits

The default experiment uses the continuous first-quarter 2024 period:

| Split | Half-open interval |
|---|---|
| Train | 2024-01-01 to 2024-03-04 |
| Validation | 2024-03-04 to 2024-03-18 |
| Test | 2024-03-18 to 2024-04-01 |

The generated local dataset contains the following reproducible evidence:

| Split | Rows | Trips represented | Zero-demand rows | First hour | Last hour |
|---|---:|---:|---:|---|---|
| Train | 396,144 | 6,307,457 | 235,897 | 2024-01-01 00:00 | 2024-03-03 23:00 |
| Validation | 87,770 | 1,656,699 | 47,819 | 2024-03-04 00:00 | 2024-03-17 23:00 |
| Test | 88,032 | 1,587,231 | 47,385 | 2024-03-18 00:00 | 2024-03-31 23:00 |
| **Total** | **571,946** | **9,551,387** | **331,101** | 2024-01-01 00:00 | 2024-03-31 23:00 |

There are 262 observed pickup zones. The validation period has one fewer local
hour because the source preserves New York wall-clock time across the March
daylight-saving transition.

Random splitting is intentionally avoided because the evaluation must simulate
training on the past and predicting a later period. The Week 3 January 2025
records are synthetic incremental-update fixtures separated from the continuous
2024 observations by a long gap. They are excluded from the baseline experiment
and can later demonstrate retraining when new data arrives.

## Assumptions and trade-offs

- Taxi zones observed at least once during the configured period define the
  eligible prediction locations. The zone lookup is treated as static context.
- Each hour present in the integrated platform defines an eligible prediction
  hour. In practice, NYC has trips every hour; using integrated hours also keeps
  weather and air-quality context aligned with the target.
- Weather observations stand in for weather forecasts that would be available
  to a production planning system.
- Same-hour air-quality measurements are treated as available contextual data.
  A production pipeline could replace them with forecast or lagged readings.
- `relative_humidity_pct` and PM2.5 are used instead of the newer `humidity` and
  `aqi` fields because the latter were added in the synthetic 2025 schema and
  are not populated throughout the 2024 baseline period.
- Time boundaries use half-open intervals, which prevents overlap between
  splits and makes reruns deterministic.

## Reusability and future extensions

`hourly_taxi_demand_dataset()` is a pure DataFrame transformation and can be
unit-tested with small fixtures. `build_training_dataset()` owns Delta I/O and
uses configuration for paths, partitions, and time boundaries. This separation
allows the same transformation to be reused by the integrated-platform workflow
and by the raw-data comparison workflow required later in Week 4.

Future datasets can be added by enriching the hourly context before the final
grid is written. The feature types are declared in `configs/config.yaml`.
Adding an ordinary numerical or categorical input therefore requires a config
entry rather than a new encoding implementation. A feature requiring a new
derivation can be added to the first SQL transformation while the imputation,
scaling, encoding, assembly, persistence, and split-safe fitting stages remain
unchanged.

Retraining reruns the two commands against refreshed integrated Delta tables.
Task 1 rebuilds the deterministic dataset and Task 2 refits the preprocessing
statistics and category dictionaries on the new training period. The saved
PipelineModel makes the exact preprocessing reusable by Task 3 and by later
inference jobs.

## Tests

Run the complete repository suite:

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/pytest -q -p no:cacheprovider
```

The Task 1–2 tests verify:

- correct hourly counts;
- preservation of zero-demand examples;
- one row per zone-hour key;
- non-negative labels;
- deterministic chronological splits;
- exclusion of records outside the configured period;
- validation of the integrated-table input contract;
- reproducible Delta input and output;
- correct temporal feature generation;
- median imputation and numerical scaling;
- consistent feature-vector width;
- support for categories unseen outside the training split;
- fitting category dictionaries only on training rows; and
- persistence and reload of the fitted Spark PipelineModel.

## Task 3 handoff

Task 3 can load `ml/hourly_taxi_demand_prepared`, filter by `split`, and pass
the `features` and `demand` columns directly to a Spark MLlib regressor. If the
training and feature stages need to be combined into one end-to-end model, it
can reuse `build_feature_pipeline(config)` or load the saved PipelineModel.
Model evaluation should preserve the chronological splits rather than creating
a new random split.
