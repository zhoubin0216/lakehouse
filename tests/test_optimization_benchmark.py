import json
import math

import pytest
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from src.data_analysis.optimization_benchmark import Variant, run_benchmark
from src.data_analysis.result_validation import ResultSnapshot, compare_results


SCHEMA = StructType([StructField("key", StringType()), StructField("value", DoubleType())])


def snapshot(rows, schema=SCHEMA):
    return ResultSnapshot.build(schema, rows)


def test_validation_order_duplicates_and_types():
    expected = snapshot([("A", 1.0), ("A", 1.0), ("B", 2.0)])
    actual = snapshot([("B", 2.0), ("A", 1.0), ("A", 1.0)])
    assert compare_results(expected, actual)["exact_hash_equal"]
    assert not compare_results(expected, snapshot([("A", 1.0), ("B", 2.0), ("B", 2.0)]))["results_equal"]
    wrong_type = StructType([StructField("key", StringType()), StructField("value", IntegerType())])
    assert not compare_results(expected, snapshot([("A", 1), ("A", 1), ("B", 2)], wrong_type))["schema_equal"]


def test_float_tolerance_null_nan_inf():
    expected = snapshot([("A", 1.0), ("B", None), ("C", float("nan")), ("D", math.inf)])
    actual = snapshot([("D", math.inf), ("C", float("nan")), ("B", None), ("A", 1.0000001)])
    check = compare_results(expected, actual)
    assert check["results_equal"] and not check["exact_hash_equal"]
    assert not compare_results(snapshot([("A", None)]), snapshot([("A", 0.0)]))["results_equal"]
    assert not compare_results(snapshot([("A", math.inf)]), snapshot([("A", -math.inf)]))["results_equal"]
    with pytest.raises(ValueError):
        compare_results(expected, actual, atol=-1)


def test_tolerance_matching_not_greedy():
    schema = StructType([StructField("x", DoubleType()), StructField("y", DoubleType())])
    left = snapshot([(0.0, 0.0), (0.2, 0.2)], schema)
    right = snapshot([(0.1, 0.1), (-0.1, -0.1)], schema)
    assert compare_results(left, right, atol=0.15, rtol=0)["results_equal"]
    assert not compare_results(left, right, atol=0.01, rtol=0)["results_equal"]


def test_row_cap():
    with pytest.raises(ValueError, match="max_result_rows"):
        ResultSnapshot.build(SCHEMA, [("A", 1.0), ("B", 2.0)], max_rows=1)


def test_runner_artifacts_cache_and_config_restore(spark, tmp_path):
    spark.sql("SELECT * FROM VALUES (1), (1), (2) AS t(value)").createOrReplaceTempView("bench_fixture")
    original = spark.conf.get("spark.sql.adaptive.enabled")
    variants = [Variant("baseline"), Variant("cache", cache_views=("bench_fixture",))]
    root = run_benchmark(spark, {"counts": "SELECT value, COUNT(*) n FROM bench_fixture GROUP BY value"},
                         tmp_path, {"warmup_runs": 0, "measured_runs": 2}, variants)
    assert json.loads((root / "status.json").read_text())["status"] == "completed"
    assert spark.conf.get("spark.sql.adaptive.enabled") == original
    assert not spark.catalog.isCached("bench_fixture")
    assert "In-memory" in (root / "cache/plans/counts_final.txt").read_text()
    assert (root / "baseline/plans/counts_initial.txt").is_file()
    assert (root / "benchmark_results.csv").is_file()
    config = json.loads((root / "cache/configuration.json").read_text())
    assert config["cache_rows"]["bench_fixture"] == 3
    assert config["storage"]
    checks = (root / "validation_results.csv").read_text().splitlines()
    assert len(checks) == 9  # two scopes x two runs x two variants + header


def test_runner_failure_keeps_diagnostics(spark, tmp_path):
    original = spark.conf.get("spark.sql.adaptive.enabled")
    variants = [Variant("baseline"), Variant("wrong", sql_overrides={"answer": "SELECT 2 AS n"})]
    with pytest.raises(AssertionError, match="validation failed"):
        run_benchmark(spark, {"answer": "SELECT 1 AS n"}, tmp_path,
                      {"warmup_runs": 0, "measured_runs": 1}, variants)
    root = next(tmp_path.iterdir())
    assert json.loads((root / "status.json").read_text())["status"] == "failed"
    assert "False" in (root / "validation_results.csv").read_text()
    assert spark.conf.get("spark.sql.adaptive.enabled") == original


def test_runner_captures_aqe_final_plan(spark, tmp_path):
    variants = [Variant("baseline"), Variant("adaptive", spark_config={
        "spark.sql.adaptive.enabled": "true"})]
    root = run_benchmark(spark, {"counts": "SELECT id % 3 AS k, COUNT(*) AS n FROM range(20) GROUP BY id % 3"},
                         tmp_path, {"warmup_runs": 0, "measured_runs": 1}, variants)
    assert "isFinalPlan=false" in (root / "adaptive/plans/counts_initial.txt").read_text()
    assert "isFinalPlan=true" in (root / "adaptive/plans/counts_final.txt").read_text()


def test_runner_rejects_invalid_runs_before_creating_output(spark, tmp_path):
    with pytest.raises(ValueError):
        run_benchmark(spark, {"answer": "SELECT 1"}, tmp_path,
                      {"measured_runs": 0}, [Variant("baseline")])
    assert not list(tmp_path.iterdir())
