"""Train, evaluate and version the Week 4 Spark ML regression pipeline."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from importlib.metadata import version
import json
import math
from pathlib import Path
import platform
import re
import subprocess
import time
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import create_spark, load_config, read_delta, table_path, write_delta
from src.machine_learning.feature_pipeline import (
    build_feature_pipeline,
    require_feature_source_columns,
)
from src.machine_learning.training_dataset import hourly_taxi_demand_dataset


SPLITS = ("train", "validation", "test")
KEYS = ("pickup_hour", "pickup_location_id")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def save_report(path: Path, report: dict) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def source_snapshot(
    spark: SparkSession, path: str, source_version: int | None = None,
) -> tuple[DataFrame, dict]:
    """Pin one Delta snapshot before any fitting or evaluation actions."""
    path = str(Path(path).resolve())
    if source_version is not None and source_version < 0:
        raise ValueError("source_version must be non-negative")
    if source_version is None:
        source_version = int(DeltaTable.forPath(spark, path).history(1).first().version)
    source = spark.read.format("delta").option("versionAsOf", source_version).load(path)
    return source, {"path": path, "delta_version": source_version}


def validate_training_dataset(dataset: DataFrame, config: dict) -> dict:
    """Reject invalid labels, keys and overlapping chronological splits."""
    ml = config["machine_learning"]
    require_feature_source_columns(dataset, ml)
    label = F.col(ml["label_column"]).cast("double")
    invalid = (
        label.isNull() | F.isnan(label) | (F.abs(label) == float("inf"))
        | (label < 0) | F.col("pickup_hour").isNull()
        | F.col("pickup_location_id").isNull() | F.col("split").isNull()
        | ~F.col("split").isin(*SPLITS)
    )
    if dataset.filter(invalid).limit(1).count():
        raise ValueError("Training rows need finite non-negative labels, valid keys and splits")
    if dataset.groupBy(*KEYS).count().filter("count > 1").limit(1).count():
        raise ValueError("Training dataset contains duplicate zone-hour keys")

    rows = dataset.groupBy("split").agg(
        F.count("*").alias("rows"),
        F.min("pickup_hour").alias("first_hour"),
        F.max("pickup_hour").alias("last_hour"),
        F.avg(label).alias("mean_label"),
    ).collect()
    stats = {row.split: row.asDict() for row in rows}
    if set(stats) != set(SPLITS) or stats["train"]["rows"] < 2:
        raise ValueError("Require all three non-empty splits and at least two train rows")
    if not (
        stats["train"]["last_hour"] < stats["validation"]["first_hour"]
        and stats["validation"]["last_hour"] < stats["test"]["first_hour"]
    ):
        raise ValueError("Train, validation and test must be strictly chronological")
    for row in stats.values():
        row["first_hour"] = row["first_hour"].isoformat()
        row["last_hour"] = row["last_hour"].isoformat()
    return stats


def build_training_pipeline(config: dict) -> Pipeline:
    """Reuse Task 2 preprocessing and append a seeded MLlib regressor."""
    ml = config["machine_learning"]
    training = ml["training"]
    regressor = RandomForestRegressor(
        labelCol=ml["label_column"], featuresCol=ml["features_column"],
        predictionCol="prediction", seed=training["seed"],
        numTrees=training["num_trees"], maxDepth=training["max_depth"],
        maxBins=training["max_bins"],
    )
    return Pipeline(stages=[*build_feature_pipeline(config).getStages(), regressor])


def fit_training_pipeline(dataset: DataFrame, config: dict) -> PipelineModel:
    """Fit both preprocessing and regression only on the training split."""
    partitions = config["machine_learning"]["training"]["fit_partitions"]
    training = (
        dataset.filter(F.col("split") == "train")
        .repartition(partitions, *KEYS).sortWithinPartitions(*KEYS).cache()
    )
    try:
        return build_training_pipeline(config).fit(training)
    finally:
        training.unpersist()


def evaluate_predictions(predictions: DataFrame, label_column: str) -> dict:
    """Compute regression metrics and a training-mean baseline in one pass."""
    label = F.col(label_column).cast("double")
    aggregates = [F.count("*").alias("rows"), F.var_pop(label).alias("variance")]
    for name, column in (("model", "prediction"), ("mean_baseline", "baseline_prediction")):
        error = F.col(column) - label
        aggregates.extend([
            F.avg(error * error).alias(f"{name}_mse"),
            F.avg(F.abs(error)).alias(f"{name}_mae"),
        ])
    results = {}
    for row in predictions.groupBy("split").agg(*aggregates).collect():
        metrics = {"rows": row.rows}
        for name in ("model", "mean_baseline"):
            mse, mae = row[f"{name}_mse"], row[f"{name}_mae"]
            if not all(value is not None and math.isfinite(value) for value in (mse, mae)):
                raise ValueError("Evaluation produced non-finite metrics")
            metrics[name] = {
                "rmse": math.sqrt(mse), "mae": mae,
                "r2": 1.0 - mse / row.variance if row.variance and row.variance > 0 else None,
            }
        results[row.split] = metrics
    return results


def code_revision() -> dict:
    root = Path(__file__).resolve().parents[2]
    try:
        revision = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=root, text=True, stderr=subprocess.DEVNULL,
        ).strip()
        dirty = subprocess.check_output(
            ["git", "status", "--porcelain", "--", "src", "configs", "requirements.txt"],
            cwd=root, text=True, stderr=subprocess.DEVNULL,
        ).strip()
        return {"commit": revision, "working_tree_modified": bool(dirty)}
    except (OSError, subprocess.CalledProcessError):
        return {"commit": None, "working_tree_modified": None}


def run_training(
    spark: SparkSession, config: dict, *, run_id: str | None = None,
    rebuild_dataset: bool = False, source_version: int | None = None,
) -> dict:
    """Create an immutable run; mark complete only after all artifacts exist.

    source_version selects the Task 1 table, or the integrated table when
    rebuild_dataset is true. Rebuilding writes a run-local Task 1 dataset and
    never overwrites the shared Task 1/2 outputs.
    """
    ml = config["machine_learning"]
    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-") + uuid4().hex[:8]
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", run_id):
        raise ValueError("run_id must contain 1-80 letters, digits, underscores or hyphens")
    run_path = Path(table_path(config, ml["training"]["runs_root"])).resolve() / run_id
    run_path.mkdir(parents=True, exist_ok=False)
    report_path = run_path / "run.json"
    started = time.perf_counter()
    report = {
        "run_id": run_id, "status": "running", "started_at": utc_now(),
        "run_path": str(run_path), "config": config, "code": code_revision(),
        "runtime": {
            "spark": spark.version, "delta-spark": version("delta-spark"),
            "numpy": version("numpy"), "session_timezone": spark.conf.get("spark.sql.session.timeZone"),
            "python": platform.python_version(), "master": spark.sparkContext.master,
        },
        "rebuild_dataset": rebuild_dataset,
        "evaluation_caveat": "Same-hour observed weather and air quality are proxies, not point-in-time forecasts.",
    }
    save_report(report_path, report)
    dataset = predictions = None
    try:
        # Preserve executable ML code as well as the commit ID for dirty worktrees.
        code_path = run_path / "code"
        code_path.mkdir()
        for filename in ("training.py", "training_dataset.py", "feature_pipeline.py"):
            (code_path / filename).write_bytes(Path(__file__).with_name(filename).read_bytes())
        (code_path / "common.py").write_bytes(Path(__file__).parents[1].joinpath("common.py").read_bytes())
        source_key = "source_table" if rebuild_dataset else "training_dataset_table"
        source, report["source"] = source_snapshot(
            spark, table_path(config, ml[source_key]), source_version,
        )
        report["source"]["kind"] = "integrated" if rebuild_dataset else "task1_dataset"
        if rebuild_dataset:
            dataset_path = str(run_path / "training_dataset")
            write_delta(hourly_taxi_demand_dataset(source, ml), dataset_path, partitions=["split"])
            dataset = read_delta(spark, dataset_path)
            report["training_dataset"] = {"path": dataset_path, "delta_version": 0}
        else:
            dataset = source
            report["training_dataset"] = report["source"].copy()
        dataset = dataset.cache()
        report["input_schema"] = dataset.schema.jsonValue()
        report["splits"] = validate_training_dataset(dataset, config)
        report["dataset_seconds"] = time.perf_counter() - started
        save_report(report_path, report)

        print(f"Training run {run_id}: fitting preprocessing and RandomForest on train only", flush=True)
        fit_started = time.perf_counter()
        model = fit_training_pipeline(dataset, config)
        report["fit_seconds"] = time.perf_counter() - fit_started
        prediction_columns = list(dict.fromkeys([
            *ml["output_metadata_columns"], ml["label_column"], "split", "prediction",
        ]))
        predictions = model.transform(dataset).select(*prediction_columns).withColumn(
            "baseline_prediction", F.lit(report["splits"]["train"]["mean_label"]),
        ).cache()
        report["metrics"] = evaluate_predictions(predictions, ml["label_column"])
        report["model_path"] = str(run_path / "model")
        report["predictions_path"] = str(run_path / "predictions")
        model.write().save(report["model_path"])
        write_delta(predictions, report["predictions_path"], partitions=["split"])
        report["feature_count"] = model.stages[-1].numFeatures
        report["model_parameters"] = {
            param.name: value for param, value in model.stages[-1].extractParamMap().items()
        }
        report["status"] = "complete"
    except Exception as error:
        report["status"] = "failed"
        report["error"] = {"type": type(error).__name__, "message": str(error)}
        raise
    finally:
        report["finished_at"] = utc_now()
        report["total_seconds"] = time.perf_counter() - started
        save_report(report_path, report)
        if predictions is not None:
            predictions.unpersist()
        if dataset is not None:
            dataset.unpersist()
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/config.yaml"))
    parser.add_argument("--run-id", help="Unique output name; existing runs are never overwritten")
    parser.add_argument("--rebuild-dataset", action="store_true", help="Build Task 1 data from integrated into this run")
    parser.add_argument("--source-version", type=int, help="Pin input Delta version instead of latest")
    args = parser.parse_args()
    config = load_config(args.config)
    spark = create_spark()
    try:
        report = run_training(
            spark, config, run_id=args.run_id, rebuild_dataset=args.rebuild_dataset,
            source_version=args.source_version,
        )
        print(json.dumps({"run_path": report["run_path"], "metrics": report["metrics"]}, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
