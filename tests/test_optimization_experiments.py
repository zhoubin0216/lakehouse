import csv
import json

from src.data_analysis.execution_metrics import executed_metrics, scan_summary
from src.data_analysis.optimization_benchmark import Variant, run_benchmark
from src.data_analysis.optimization_queries import broadcast_queries, pruning_queries, replace_once
from src.data_analysis.optimization_suite import select_candidates
from src.data_analysis.query_library import ANALYTICAL_QUERIES
from src.data_analysis.result_validation import ResultSnapshot, compare_results
import pytest


def install_views(spark):
    spark.sql("""SELECT *, CAST(pickup_timestamp AS DATE) pickup_date,
        pickup_timestamp AS pickup_hour FROM VALUES
        (2024, 1, 1, 'A', 'X', TIMESTAMP_NTZ '2024-01-01 10:00:00', 1, 2.0D),
        (2024, 1, 1, 'A', 'X', TIMESTAMP_NTZ '2024-01-01 10:00:00', 1, 4.0D),
        (2024, 2, 2, 'B', 'Y', TIMESTAMP_NTZ '2024-02-01 12:00:00', 2, 6.0D)
        AS t(pickup_year, pickup_month, pickup_location_id, pickup_zone,
             pickup_borough, pickup_timestamp, weather_condition_code, trip_distance)
    """).createOrReplaceTempView("integrated_taxi_trips")
    spark.sql("""SELECT * FROM VALUES
        (TIMESTAMP_NTZ '2024-01-01 10:00:00', 1),
        (TIMESTAMP_NTZ '2024-01-01 11:00:00', 2),
        (TIMESTAMP_NTZ '2024-02-01 12:00:00', 2)
        AS t(event_hour, weather_condition_code)""").createOrReplaceTempView("analysis_weather")
    spark.sql("""SELECT * FROM VALUES
        (TIMESTAMP_NTZ '2024-01-01 10:00:00', 10.0D),
        (TIMESTAMP_NTZ '2024-01-01 11:00:00', 20.0D),
        (TIMESTAMP_NTZ '2024-02-01 12:00:00', 30.0D)
        AS t(event_hour, pm25_avg_ug_m3)""").createOrReplaceTempView("analysis_air_quality")


def test_broadcast_sql_equivalence(spark):
    install_views(spark)
    for name, sql in broadcast_queries().items():
        baseline = spark.sql(ANALYTICAL_QUERIES[name])
        optimized = spark.sql(sql)
        assert compare_results(ResultSnapshot.build(baseline.schema, baseline.collect()),
                               ResultSnapshot.build(optimized.schema, optimized.collect()))["results_equal"]
    assert "BROADCAST(w)" in broadcast_queries()["zone_weather_variation"]
    assert broadcast_queries()["monthly_zone_demand"] == ANALYTICAL_QUERIES["monthly_zone_demand"]


def test_pruning_equivalence_and_year_rollover(spark):
    install_views(spark)
    baseline, optimized = pruning_queries()
    for name in baseline:
        left, right = spark.sql(baseline[name]), spark.sql(optimized[name])
        assert compare_results(ResultSnapshot.build(left.schema, left.collect()),
                               ResultSnapshot.build(right.schema, right.collect()))["results_equal"]
        assert right.collect()[0].trip_count == 2
    december, _ = pruning_queries(2024, 12)
    assert "2025-01-01" in december["monthly_zone_range_filter"]
    with pytest.raises(ValueError):
        replace_once("SELECT 1", "not present", "SELECT 2")


def test_metrics_export_real_scan_and_aqe(spark, tmp_path):
    path = str(tmp_path / "source")
    spark.range(20).coalesce(1).write.parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("metrics_source")
    original = spark.conf.get("spark.sql.adaptive.enabled")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    try:
        df = spark.sql("SELECT id % 3 k, COUNT(*) n FROM metrics_source GROUP BY id % 3")
        df.collect()
        metrics = executed_metrics(df)
        scans = scan_summary(metrics)
        assert sum(s["files"] for s in scans) == 1
        assert any(n["class"] == "AdaptiveSparkPlanExec" for n in metrics["nodes"])
        assert any(n["class"] == "FileSourceScanExec" for n in metrics["nodes"])
    finally:
        spark.conf.set("spark.sql.adaptive.enabled", original)


def test_runner_handles_baseline_last(spark, tmp_path):
    variants = [Variant("adaptive", {"spark.sql.adaptive.enabled": "true"}), Variant("baseline")]
    root = run_benchmark(spark, {"answer": "SELECT 1 AS n"}, tmp_path,
                         {"warmup_runs": 0, "measured_runs": 1}, variants)
    with (root / "benchmark_results.csv").open() as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["variant"] == "adaptive"
    assert float(rows[0]["speedup"]) > 0
    assert json.loads((root / "status.json").read_text())["all_results_equal"]


def test_selection_rejects_inapplicable_and_unstable_improvements():
    def row(name, variant, seconds, speedup, wins):
        return {"experiment": "full_queries", "query_name": name, "variant": variant,
                "median_seconds": seconds, "speedup": speedup, "block_wins": wins, "blocks": 3}
    selected = select_candidates([
        row("monthly_zone_demand", "broadcast", 0.1, 10, 3),
        row("monthly_zone_demand", "aqe", 0.2, 2, 1),
        row("zone_weather_variation", "broadcast", 0.4, 2, 3),
        row("zone_weather_variation", "combined", 0.3, 3, 3),
        row("avg_distance_by_weather", "cache", 0.9, 1.01, 3),
    ])
    assert selected["monthly_zone_demand"] == "baseline"
    assert selected["zone_weather_variation"] == "combined"
    assert selected["avg_distance_by_weather"] == "baseline"
