"""Controlled Week 2 benchmarks. Never modifies the original SQL library."""
from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass, field
from datetime import datetime, timezone
from importlib.metadata import version
import hashlib
import json
import math
from pathlib import Path
import platform
import statistics
import subprocess
import time
import uuid

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import functions as F

from src.common import create_spark, load_config, table_path
from src.data_analysis.query_library import ANALYTICAL_QUERIES, INTEGRATED_VIEW
from src.data_analysis.result_validation import ResultSnapshot, compare_results
from src.data_analysis.execution_metrics import executed_metrics
from src.data_analysis.optimization_queries import broadcast_queries

BASE_CONFIG = {
    "spark.sql.adaptive.enabled": "false",
    "spark.sql.autoBroadcastJoinThreshold": "-1",
    "spark.sql.adaptive.autoBroadcastJoinThreshold": "-1",
}
CONFIG_KEYS = (
    *BASE_CONFIG, "spark.sql.shuffle.partitions", "spark.sql.files.maxPartitionBytes",
    "spark.sql.session.timeZone", "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.skewJoin.enabled", "spark.sql.adaptive.advisoryPartitionSizeInBytes",
    "spark.sql.inMemoryColumnarStorage.compressed", "spark.sql.inMemoryColumnarStorage.batchSize",
)


@dataclass(frozen=True)
class Variant:
    name: str
    spark_config: dict = field(default_factory=dict)
    cache_views: tuple[str, ...] = ()
    # Future experiments can supply logically equivalent SQL without editing
    # the original files. Each query must share its baseline's logical scope.
    sql_overrides: dict = field(default_factory=dict)


VARIANTS = {
    "baseline": Variant("baseline"),
    "cache": Variant("cache", cache_views=(INTEGRATED_VIEW,)),
    "broadcast": Variant("broadcast", sql_overrides=broadcast_queries()),
    "aqe": Variant("aqe", spark_config={"spark.sql.adaptive.enabled": "true"}),
    "combined": Variant("combined", spark_config={"spark.sql.adaptive.enabled": "true"},
                        sql_overrides=broadcast_queries()),
}


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, ensure_ascii=False, default=str) + "\n",
                          encoding="utf-8")


def write_csv(path, records):
    if not records:
        return
    with Path(path).open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)


def effective_config(spark):
    return {key: spark.conf.get(key) for key in CONFIG_KEYS}


def prepare_inputs(spark, config):
    """Pin all input Delta versions so every variant reads the same snapshot."""
    specs = (
        (INTEGRATED_VIEW, config["data_integration"]["integrated_taxi_trips_table"]),
        ("analysis_weather", config["datasets"]["weather_hourly"]["normal_table"]),
        ("analysis_air_quality", config["datasets"]["air_quality"]["normal_table"]),
    )
    manifests, frames = [], {}
    for view, relative in specs:
        path = str(Path(table_path(config, relative)).resolve())
        table = DeltaTable.forPath(spark, path)
        history = table.history(1).select("version", "timestamp").first()
        detail = table.detail().first().asDict()
        if table.history(1).select("version").first()[0] != history.version:
            raise RuntimeError(f"Input changed during snapshot capture: {path}; retry")
        df = spark.read.format("delta").option("versionAsOf", history.version).load(path)
        frames[view] = df
        manifests.append({"view": view, "path": path, "delta_version": history.version,
                          "delta_commit_timestamp": history.timestamp,
                          "size_bytes": detail["sizeInBytes"], "active_files": detail["numFiles"],
                          "partition_columns": detail["partitionColumns"],
                          "schema": df.schema.jsonValue()})
    frames[INTEGRATED_VIEW].createOrReplaceTempView(INTEGRATED_VIEW)
    bounds = spark.sql(f"SELECT MIN(pickup_hour) first_hour, MAX(pickup_hour) last_hour, "
                       f"COUNT(*) trip_count FROM {INTEGRATED_VIEW}").first()
    for view in ("analysis_weather", "analysis_air_quality"):
        # Keep local NTZ semantics, independent of host/session timezone.
        df = frames[view].filter(F.col("event_hour").between(
            F.lit(bounds.first_hour).cast("timestamp_ntz"),
            F.lit(bounds.last_hour).cast("timestamp_ntz")))
        df.createOrReplaceTempView(view)
    for manifest in manifests:
        manifest["analysis_rows"] = (bounds.trip_count if manifest["view"] == INTEGRATED_VIEW
                                     else spark.table(manifest["view"]).count())
    return manifests


def storage_snapshot(spark):
    return [{"rdd_id": item.id(), "name": item.name(), "partitions": item.numPartitions(),
             "cached_partitions": item.numCachedPartitions(),
             "memory_bytes": item.memSize(), "disk_bytes": item.diskSize()}
            for item in spark.sparkContext._jsc.sc().getRDDStorageInfo()]


def environment(spark, inputs):
    def git(*args):
        try:
            return subprocess.check_output(["git", *args], text=True, stderr=subprocess.DEVNULL).strip()
        except (OSError, subprocess.CalledProcessError):
            return None
    return {"created_at_utc": datetime.now(timezone.utc).isoformat(),
            "spark_version": spark.version, "delta_version": version("delta-spark"),
            "python_version": platform.python_version(), "platform": platform.platform(),
            "java_version": spark._jvm.java.lang.System.getProperty("java.version"),
            "spark_master": spark.sparkContext.master,
            "driver_memory": spark.sparkContext.getConf().get("spark.driver.memory", "unknown"),
            "git_commit": git("rev-parse", "HEAD"), "git_status": git("status", "--short"),
            "implementation_sha256": {
                p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                for p in Path(__file__).parent.glob("*.py")},
            "original_spark_config": effective_config(spark), "inputs": inputs,
            "methodology": {
                "timing": "spark.sql construction + collect; validation and plan export excluded",
                "access": "warm repeated access; OS file cache is NOT cleared",
                "order": "CLI baseline-first; suite randomizes variant order across independent blocks; queries in registry order",
                "cache_cost": "cache registration + full count materialization, reported separately",
                "result_scope": "small aggregated results collected to driver; not a raw-trip benchmark",
                "schema_check": "ordered names and SQL types; nullability and metadata ignored",
                "hash": "exact SHA-256, order-independent, preserves duplicate rows",
            }}


def run_benchmark(spark, queries, output_root, settings, variants, inputs=()):
    warmups = settings.get("warmup_runs", 1)
    repeats = settings.get("measured_runs", 5)
    atol, rtol = settings.get("absolute_tolerance", 1e-8), settings.get("relative_tolerance", 1e-6)
    max_rows = settings.get("max_result_rows", 100_000)
    if not isinstance(warmups, int) or not isinstance(repeats, int) or warmups < 0 or repeats < 1:
        raise ValueError("warmup_runs >= 0 and measured_runs >= 1 are required")
    if not isinstance(max_rows, int) or max_rows < 1:
        raise ValueError("max_result_rows must be positive")
    if not math.isfinite(atol) or not math.isfinite(rtol) or atol < 0 or rtol < 0:
        raise ValueError("Tolerances must be finite and non-negative")
    if not queries or any(not name.replace("_", "").isalnum() for name in queries):
        raise ValueError("Query names must be non-empty safe identifiers")
    if sum(v.name == "baseline" for v in variants) != 1:
        raise ValueError("Exactly one baseline variant is required")
    names = [v.name for v in variants]
    if len(set(names)) != len(names) or any(not n.replace("_", "").isalnum() for n in names):
        raise ValueError("Variant names must be unique safe identifiers")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    root = Path(output_root) / run_id
    root.mkdir(parents=True, exist_ok=False)
    manifest = environment(spark, inputs)
    manifest.update({"run_id": run_id, "settings": settings,
                     "variant_order": [v.name for v in variants],
                     "query_sha256": {name: hashlib.sha256(sql.encode()).hexdigest()
                                      for name, sql in queries.items()}})
    write_json(root / "environment.json", manifest)
    write_json(root / "status.json", {"status": "running"})
    print(f"Benchmark output: {root}", flush=True)
    changed = set(BASE_CONFIG) | set(settings.get("spark_config", {}))
    changed.update(key for variant in variants for key in variant.spark_config)
    original = {key: spark.conf.get(key, None) for key in changed}
    summaries, timings, validations, references, baseline_times = [], [], [], {}, {}
    try:
        # Obtain an untimed correctness reference before randomized variants.
        # This also warms access: NO cold-disk claim is made.
        baseline = next(v for v in variants if v.name == "baseline")
        for key, value in {**BASE_CONFIG, **settings.get("spark_config", {}),
                           **baseline.spark_config}.items():
            spark.conf.set(key, str(value))
        spark.catalog.clearCache()
        (root / "references").mkdir()
        for name, sql in queries.items():
            df = spark.sql(baseline.sql_overrides.get(name, sql))
            references[name] = ResultSnapshot.build(df.schema, df.collect(), max_rows)
            write_json(root / "references" / f"{name}.json", references[name].artifact())
        for variant in variants:
            spark.catalog.clearCache()
            # Reset ALL changed keys before each variant to prevent leakage.
            for key, value in original.items():
                if value is None:
                    spark.conf.unset(key)
                else:
                    spark.conf.set(key, value)
            conf = {**BASE_CONFIG, **settings.get("spark_config", {}), **variant.spark_config}
            for key, value in conf.items():
                spark.conf.set(key, str(value))
            variant_root = root / variant.name
            variant_root.mkdir()
            (variant_root / "plans").mkdir()
            (variant_root / "results").mkdir()
            (variant_root / "metrics").mkdir()
            cache_build = 0.0
            cache_rows = {}
            if variant.cache_views:
                started = time.perf_counter()
                for view in variant.cache_views:
                    spark.catalog.cacheTable(view, storageLevel=StorageLevel.MEMORY_AND_DISK)
                    cache_rows[view] = spark.table(view).count()
                cache_build = time.perf_counter() - started
            write_json(variant_root / "configuration.json", {
                "spark_config": effective_config(spark), "overrides": conf,
                "cache_views": variant.cache_views, "cache_rows": cache_rows,
                "cache_build_seconds": cache_build, "storage": storage_snapshot(spark)})
            for name, original_sql in queries.items():
                sql = variant.sql_overrides.get(name, original_sql)
                (variant_root / f"{name}.sql").write_text(sql + "\n", encoding="utf-8")
                initial = spark.sql("EXPLAIN FORMATTED " + sql).collect()
                plan_root = variant_root / "plans"
                (plan_root / f"{name}_initial.txt").write_text(
                    "\n".join(row[0] for row in initial), encoding="utf-8")
                for _ in range(warmups):
                    spark.sql(sql).collect()
                durations, first = [], None
                for index in range(1, repeats + 1):
                    started = time.perf_counter()
                    result = spark.sql(sql)
                    rows = result.collect()
                    elapsed = time.perf_counter() - started
                    durations.append(elapsed)
                    snapshot = ResultSnapshot.build(result.schema, rows, max_rows)
                    if first is None:
                        first = snapshot
                    checks = (("within_variant", first), ("against_baseline", references[name]))
                    for scope, expected in checks:
                        check = compare_results(expected, snapshot, atol=atol, rtol=rtol)
                        validations.append({"query_name": name, "variant": variant.name,
                                            "run": index, "scope": scope, **check})
                    timings.append({"query_name": name, "variant": variant.name,
                                    "run": index, "seconds": elapsed})
                    write_json(variant_root / "metrics" / f"{name}_run_{index}.json",
                               executed_metrics(result))
                    print(f"{variant.name}/{name} run {index}/{repeats}: {elapsed:.4f}s", flush=True)
                    if not all(v["results_equal"] for v in validations[-2:]):
                        write_json(variant_root / "results" / f"{name}_failed_run_{index}.json",
                                   snapshot.artifact())
                        raise AssertionError(f"Result validation failed: {variant.name}/{name}/{index}")
                # Capture the SAME DataFrame AFTER its measured action: AQE
                # output now contains the final plan, not a fresh initial plan.
                final = spark._jvm.PythonSQLUtils.explainString(result._jdf.queryExecution(), "formatted")
                (plan_root / f"{name}_final.txt").write_text(final, encoding="utf-8")
                write_json(variant_root / "results" / f"{name}.json", snapshot.artifact())
                median = statistics.median(durations)
                if variant.name == "baseline":
                    baseline_times[name] = median
                summaries.append({"query_name": name, "variant": variant.name,
                                  "warmup_runs": warmups, "measured_runs": repeats,
                                  "median_seconds": median, "mean_seconds": statistics.mean(durations),
                                  "min_seconds": min(durations), "max_seconds": max(durations),
                                  "baseline_median_seconds": baseline_times.get(name),
                                  "speedup": baseline_times.get(name, 0) / median if name in baseline_times else None,
                                  "cache_build_seconds_shared": cache_build,
                                  "result_rows": len(snapshot.rows), "result_hash": snapshot.content_hash,
                                  "results_equal": True,
                                  "aqe_enabled": spark.conf.get("spark.sql.adaptive.enabled"),
                                  "broadcast_threshold": spark.conf.get("spark.sql.autoBroadcastJoinThreshold"),
                                  "cache_enabled": bool(variant.cache_views),
                                  "initial_plan": str(plan_root.relative_to(root) / f"{name}_initial.txt"),
                                  "final_plan": str(plan_root.relative_to(root) / f"{name}_final.txt")})
                write_csv(root / "benchmark_results.csv", summaries)
                write_csv(root / "timings.csv", timings)
                write_csv(root / "validation_results.csv", validations)
            # Block until cached blocks are released before the next variant.
            for view in variant.cache_views:
                spark.table(view).unpersist(blocking=True)
            spark.catalog.clearCache()
        for record in summaries:
            record["baseline_median_seconds"] = baseline_times[record["query_name"]]
            record["speedup"] = (record["baseline_median_seconds"] / record["median_seconds"]
                                 if record["median_seconds"] else None)
        write_csv(root / "benchmark_results.csv", summaries)
        write_json(root / "status.json", {"status": "completed", "all_results_equal": True})
    except Exception as error:
        write_csv(root / "timings.csv", timings)
        write_csv(root / "validation_results.csv", validations)
        write_json(root / "status.json", {"status": "failed", "error": str(error)})
        raise
    finally:
        spark.catalog.clearCache()
        for key, value in original.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)
    print(f"Completed: {root}", flush=True)
    return root


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/config.yaml"))
    parser.add_argument("--query", nargs="+", choices=list(ANALYTICAL_QUERIES))
    parser.add_argument("--variant", nargs="+", choices=list(VARIANTS), default=["baseline"])
    parser.add_argument("--output-root", type=Path)
    args = parser.parse_args()
    if len(set(args.variant)) != len(args.variant) or (args.query and len(set(args.query)) != len(args.query)):
        parser.error("Duplicate query or variant names are not allowed")
    config = load_config(args.config)
    settings = config.get("data_analysis", {}).get("optimization_benchmark", {})
    root = args.output_root or Path(table_path(config, settings.get("results_root", "analysis/optimization")))
    queries = {name: ANALYTICAL_QUERIES[name] for name in args.query or ANALYTICAL_QUERIES}
    variants = [VARIANTS["baseline"]] + [VARIANTS[n] for n in args.variant if n != "baseline"]
    spark = create_spark()
    try:
        inputs = prepare_inputs(spark, config)
        run_benchmark(spark, queries, root, settings, variants, inputs)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
