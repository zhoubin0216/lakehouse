from copy import deepcopy
from datetime import datetime
import json

import pytest
from pyspark.ml import PipelineModel
from pyspark.sql import functions as F

from src.common import load_config, read_delta, table_path, validate_config, write_delta
from src.machine_learning.training import (
    evaluate_predictions,
    run_training,
    validate_training_dataset,
)
from src.machine_learning.training_dataset import REQUIRED_SOURCE_COLUMNS


@pytest.fixture
def training_config(tmp_path):
    config = deepcopy(load_config())
    config["paths"]["lakehouse"] = str(tmp_path / "lakehouse")
    ml = config["machine_learning"]
    ml["numerical_features"] = ["temperature_c"]
    ml["categorical_features"] = ["pickup_location_id", "hour_of_day"]
    ml["training"].update(num_trees=3, max_depth=2, fit_partitions=2)
    return config


@pytest.fixture
def training_rows(spark):
    return spark.createDataFrame([
        (datetime(2024, 1, 1, 8), 1, 10.0, True, True, 2.0, "train"),
        (datetime(2024, 1, 2, 9), 2, 20.0, True, True, 6.0, "train"),
        (datetime(2024, 3, 5, 8), 99, None, False, False, 4.0, "validation"),
        (datetime(2024, 3, 6, 9), 2, 30.0, True, True, 10.0, "validation"),
        (datetime(2024, 3, 19, 8), 99, 40.0, True, True, 5.0, "test"),
        (datetime(2024, 3, 20, 9), 2, 50.0, True, True, 12.0, "test"),
    ], """pickup_hour timestamp, pickup_location_id int, temperature_c double,
            weather_available boolean, air_quality_available boolean,
            demand double, split string""")


@pytest.mark.parametrize("change", ["empty", "negative", "nan", "duplicate", "chronology"])
def test_reject_invalid_training_data(training_rows, training_config, change):
    source = training_rows
    if change == "empty":
        source = source.filter("split != 'test'")
    elif change in ("negative", "nan"):
        source = source.withColumn("demand", F.lit(-1.0 if change == "negative" else float("nan")))
    elif change == "duplicate":
        source = source.unionByName(source.limit(1))
    elif change == "chronology":
        source = source.withColumn("split", F.when(F.col("split") == "train", "test").when(
            F.col("split") == "test", "train").otherwise(F.col("split")))
    with pytest.raises(ValueError):
        validate_training_dataset(source, training_config)


def test_metrics_and_constant_target_r2(spark):
    predictions = spark.createDataFrame([
        (1.0, 2.0, 0.0, "test"), (3.0, 2.0, 0.0, "test"),
        (5.0, 4.0, 0.0, "validation"), (5.0, 6.0, 0.0, "validation"),
    ], "demand double, prediction double, baseline_prediction double, split string")
    metrics = evaluate_predictions(predictions, "demand")
    assert metrics["test"]["model"] == {"rmse": 1.0, "mae": 1.0, "r2": 0.0}
    assert metrics["test"]["mean_baseline"]["mae"] == 2.0
    assert metrics["validation"]["model"]["r2"] is None


def test_training_saves_reloadable_pipeline_and_pins_snapshot(
    spark, training_config, training_rows,
):
    config = training_config
    source_path = table_path(config, config["machine_learning"]["training_dataset_table"])
    write_delta(training_rows, source_path, partitions=["split"])
    write_delta(training_rows.withColumn("demand", F.lit(100.0)), source_path, partitions=["split"])

    report = run_training(spark, config, run_id="first", source_version=0)
    assert report["status"] == "complete"
    assert report["source"]["delta_version"] == 0
    assert report["splits"]["train"]["mean_label"] == 4.0
    assert report["metrics"]["test"]["rows"] == 2
    persisted = read_delta(spark, report["predictions_path"])
    assert persisted.count() == 6
    assert persisted.select("baseline_prediction").distinct().first()[0] == 4.0

    model = PipelineModel.load(report["model_path"])
    indexer = next(stage for stage in model.stages if hasattr(stage, "labelsArray"))
    assert "99" not in indexer.labelsArray[0]
    imputer = next(stage for stage in model.stages if hasattr(stage, "surrogateDF"))
    assert imputer.surrogateDF.first()["temperature_c"] in (10.0, 20.0)
    # The complete saved pipeline can predict future rows without a label or split.
    actual = model.transform(training_rows.drop("demand", "split")).select(
        "pickup_hour", "pickup_location_id", "prediction",
    )
    expected = persisted.select(*actual.columns)
    assert actual.exceptAll(expected).count() == 0
    assert expected.exceptAll(actual).count() == 0

    with pytest.raises(FileExistsError):
        run_training(spark, config, run_id="first")
    retrained = run_training(spark, config, run_id="second", source_version=0)
    assert retrained["model_path"] != report["model_path"]
    assert retrained["metrics"] == report["metrics"]


def test_rebuild_uses_integrated_and_preserves_shared_tables(
    spark, training_config, training_rows,
):
    ml = training_config["machine_learning"]
    integrated = training_rows.drop("demand", "split")
    for column in sorted(REQUIRED_SOURCE_COLUMNS - set(integrated.columns)):
        integrated = integrated.withColumn(column, F.lit(1.0))
    integrated_path = table_path(training_config, ml["source_table"])
    write_delta(integrated, integrated_path)
    shared_path = table_path(training_config, ml["training_dataset_table"])
    write_delta(training_rows, shared_path)
    report = run_training(spark, training_config, run_id="rebuilt", rebuild_dataset=True)
    assert report["source"]["kind"] == "integrated"
    assert read_delta(spark, report["training_dataset"]["path"]).count() == 18
    assert read_delta(spark, shared_path).count() == 6
    assert report["status"] == "complete"


def test_failed_run_is_not_published(spark, training_config, training_rows):
    source_path = table_path(training_config, training_config["machine_learning"]["training_dataset_table"])
    write_delta(training_rows.filter("split = 'train'"), source_path)
    with pytest.raises(ValueError, match="three non-empty splits"):
        run_training(spark, training_config, run_id="invalid")
    from pathlib import Path
    report_path = Path(table_path(training_config, "ml/runs/invalid/run.json"))
    assert json.loads(report_path.read_text())["status"] == "failed"
    assert not (report_path.parent / "model").exists()
    with pytest.raises(ValueError, match="run_id"):
        run_training(spark, training_config, run_id="../outside")


@pytest.mark.parametrize("key,value", [("seed", True), ("num_trees", 0), ("max_depth", 31), ("max_bins", 1), ("fit_partitions", 0)])
def test_reject_invalid_model_settings(training_config, key, value):
    training_config["machine_learning"]["training"][key] = value
    with pytest.raises(ValueError, match=key):
        validate_config(training_config)


@pytest.mark.parametrize("column", ["demand", "split", "prediction"])
def test_reject_label_leakage_in_features(training_config, column):
    training_config["machine_learning"]["categorical_features"].append(column)
    with pytest.raises(ValueError, match="must not include"):
        validate_config(training_config)
