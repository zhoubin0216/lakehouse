# Week 4 Task 3: Reproducible Spark ML Pipeline

## Workflow

```text
Task 1 Delta dataset (pinned version)
  -> validate labels, zone-hour keys and chronological splits
  -> fit Task 2 preprocessing + RandomForestRegressor on train only
  -> transform train / validation / test with that fitted PipelineModel
  -> evaluate each split against a training-mean baseline
  -> save complete PipelineModel, predictions and run manifest
```

With `--rebuild-dataset`, the entry point first pins the integrated Delta
version and calls Task 1's `hourly_taxi_demand_dataset()`. It stores the result
inside the new run. It does not overwrite the shared Task 1 or Task 2 outputs,
and does not rerun consumption, cleaning, integration or benchmarks.

## Run and retrain

From the project root with the project virtual environment activated:

```bash
python -m pip install -r requirements.txt

# Build from integrated, including on a machine with no Task 1 output yet.
python -m src.machine_learning.training --rebuild-dataset

# Or train from the existing Task 1 Delta table.
python -m src.machine_learning.training

# Optional explicit run name; an existing name is rejected, never overwritten.
python -m src.machine_learning.training --rebuild-dataset --run-id experiment_02

# Reuse an exact historical input snapshot.
python -m src.machine_learning.training --rebuild-dataset --source-version 1
```

`--source-version` means an integrated-table version with `--rebuild-dataset`,
otherwise a Task 1 table version. Omitting it pins the latest committed input
version at the start of the run. `--config path/to/config.yaml` selects an
alternative configuration.

Retraining reuses this same command after integrated data is refreshed. Adjust
`observation_start`, `train_end_exclusive`, `validation_end_exclusive` and
`observation_end_exclusive` to cover the desired new period. The default Q1 2024
window deliberately excludes the synthetic January 2025 release, so merely
adding records outside that window does not change the training population.
Do not silently treat the intervening unobserved months as zero demand.

## Model and evaluation

The target remains the accepted yellow-taxi pickup count per zone-hour.
Random-forest regression provides a modest nonlinear baseline for interactions
between time, location and environmental conditions. Leaf averages and their
ensemble average preserve non-negative predictions for non-negative labels.
The goal is a reusable engineering workflow, not extensive model selection.

`machine_learning.training` controls the random seed, number of trees, maximum
depth, histogram bins, fitting partitions and output root. The default is 20
trees of maximum depth 8, with seed 2221. Task 2's configured feature processing
is reused unchanged, including scaling even though the forest does not require
it. Keeping that interface shared allows later comparison with other models.

Imputation medians, scaling statistics, category dictionaries and regression
parameters are fitted only on the training split. Unknown categories in later
splits use Task 2's existing unknown buckets. Validation and test do not enter
fitting; there is no automatic hyperparameter search or refit on validation.
If parameters are tuned manually, choose them using validation, then reserve
test for the final assessment instead of selecting runs by test scores.

Each split reports RMSE, MAE and R-squared, alongside a baseline that predicts
the training-label mean for every row. These metrics are computed with Spark
aggregations; individual observations are not collected to the driver.
R-squared is recorded as JSON null when the split's target variance is zero.
Training metrics are diagnostics, not a generalization estimate. All zone-hour
rows, including zeros, receive equal weight; trips do not weight the loss.

The Task 1 experiment uses same-hour observed weather and air quality as
proxies for available context. This is an offline conditional prediction
experiment, not a leakage-free advance-demand forecast. Production forecasting
needs point-in-time forecasts or sufficiently lagged observations; preprocessing
on train only does not remove this separate feature-availability limitation.

## Artifacts and reproducibility

Each run is isolated under `data/lakehouse/ml/runs/<run_id>/`:

```text
run.json              status, input version, schema, config, metrics, durations,
                      model parameters, software versions and code revision
code/                 copies of the ML modules and common utilities
training_dataset/     Task 1 Delta table, only with --rebuild-dataset
model/                complete Spark PipelineModel: preprocessing + regressor
predictions/          Delta table, partitioned by split; labels, keys,
                      prediction and baseline_prediction
```

The manifest begins in `running`, changes to `complete` only after saving all
artifacts, and records `failed` and the error on handled exceptions. A process
kill may leave `running` and partial artifacts; never consume such a run. Start
a fresh run ID after correcting a failure. Existing runs are never overwritten;
there is intentionally no automatically promoted "latest" model.

Input Delta versions, configuration, software versions, code copies and the
seed are recorded. Fitting repartitions and sorts by the stable zone-hour key
to reduce order-dependent variation. Reproducing a run requires the matching
code, dependencies, Spark time zone, configuration and retained input snapshot.
Do not vacuum required historical files. A fixed seed does not promise
bit-for-bit identity across Spark versions, hardware or numerical libraries.
The metadata records whether the Git working tree differed from the commit.
This local-filesystem run layout is consistent with the rest of the project;
remote storage and a production model registry are outside Task 3.

## Inference

Load the full saved pipeline, not only the regressor:

```python
import json
from pathlib import Path
from pyspark.ml import PipelineModel

run = json.loads(Path("data/lakehouse/ml/runs/<run_id>/run.json").read_text())
assert run["status"] == "complete"
model = PipelineModel.load(run["model_path"])
predictions = model.transform(future_feature_rows).select(
    "pickup_hour", "pickup_location_id", "prediction"
)
```

`future_feature_rows` must supply the Task 1 unencoded feature inputs, including
`pickup_hour` and availability flags. It need not contain `demand` or `split`.
Do not pass Task 2's already assembled `features` table through this model.
Generating future, point-in-time feature rows is distinct from the historical
Task 1 demand-label builder.

## Reuse and extension

- Dataset construction, feature transforms, fitting, evaluation and persistence
  are separate functions; the CLI only orchestrates them.
- Existing input features can be selected through the Task 2 configuration.
  New derived features use Task 2's SQL transformation. New datasets must first
  expose their features through Task 1's data contract.
- Retraining creates a fresh preprocessing/model pair and retains previous runs.
  This avoids pairing old regression coefficients/trees with new encodings.
- Other non-negative regression targets can reuse the feature pipeline and
  evaluation/persistence patterns, but require a new dataset builder, prediction
  grain, validation rules and output namespace. The current builder/validator is
  intentionally specific to zone-hour demand. Classification would additionally
  require a classifier and classification metrics.

## Tests

### Verified local full-data run

Run `week4_task3_20261002` completed from integrated Delta version 1 using the
default configuration and 352 features. The source window is Q1 2024; the
synthetic 2025 update is excluded. Artifacts and unrounded metrics are in
`data/lakehouse/ml/runs/week4_task3_20261002/run.json` and its sibling folders.

| Split | Rows | RF RMSE | RF MAE | RF R-squared | Mean-baseline RMSE |
|---|---:|---:|---:|---:|---:|
| Train | 396,144 | 33.4590 | 12.5252 | 0.5518 | 49.9778 |
| Validation | 87,770 | 38.8362 | 14.1372 | 0.5383 | 57.2343 |
| Test | 88,032 | 36.9041 | 13.5195 | 0.5403 | 54.4712 |

The model improves over the simple constant baseline, but still leaves
substantial demand variation unexplained. No parameters were selected using
these test results. The same-hour context caveat above applies to every metric.
Feature-and-model fitting took 43.74 seconds and the full run took 86.20 seconds
on this local machine, excluding Spark startup; these are single-run observations,
not a controlled benchmark. Runtime: Spark 3.5.9, Delta 3.3.3, Python 3.11.15,
NumPy 1.26.4, `local[4]`, Spark session time zone `Europe/Stockholm`. Source
`pickup_hour` is `timestamp_ntz`, preserving its local wall-clock fields.

### Regression checks

The Task 1-3 and configuration suite passed **43 tests** on this environment.
This is the focused ML suite, not the complete Weeks 1-3 regression suite.
An independent Spark process reloaded the full-data model and predicted all
571,946 rows without label/split input: there were zero differences from saved
predictions and no null, NaN or negative predictions.

```bash
python -m pytest -q tests/test_ml_training.py tests/test_ml_training_dataset.py \
  tests/test_ml_feature_pipeline.py tests/test_config.py
```

Coverage includes invalid labels/splits and duplicate keys, chronological
ordering, train-only category and imputation fitting, metric calculations,
constant-target evaluation, saved-model reload and unlabeled inference,
snapshot pinning, isolated retraining, run-local dataset rebuilding, failure
state recording and rejection of unsafe run IDs/model configuration.
