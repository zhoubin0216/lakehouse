"""Run the controlled Week 3 production-readiness evaluation suite."""
from __future__ import annotations

import argparse
from copy import deepcopy
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import platform
import statistics
import subprocess
import time
import uuid

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import create_spark, load_config, read_delta, table_path, write_delta
from src.data_analysis.data_products import PRODUCT_BUILDERS
from src.data_quality import (
    build_validation_report,
    classify_records,
    non_empty_text_rule,
    range_rule,
    required_rule,
    write_rejected_records,
)
from src.incremental.pipeline import split_novel_records
from src.monitoring.logger import record_operation, utc_now


def write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def median_seconds(samples: list[float]) -> float:
    if not samples:
        raise ValueError("At least one timing sample is required")
    return float(statistics.median(samples))


def overhead_metrics(
    baseline_samples: list[float],
    enabled_samples: list[float],
) -> dict:
    baseline = median_seconds(baseline_samples)
    enabled = median_seconds(enabled_samples)
    difference = enabled - baseline
    return {
        "baseline_samples_seconds": baseline_samples,
        "enabled_samples_seconds": enabled_samples,
        "baseline_median_seconds": baseline,
        "enabled_median_seconds": enabled,
        "overhead_seconds": difference,
        "overhead_percent": (difference / baseline * 100) if baseline else None,
    }


def synthetic_weather_rows(
    spark: SparkSession,
    count: int,
    *,
    offset: int = 0,
) -> DataFrame:
    key = F.col("id") + F.lit(offset)
    return spark.range(count).select(
        (F.lit(2000) + key).cast("int").alias("year"),
        F.lit(1).cast("int").alias("month"),
        F.lit(1).cast("int").alias("day"),
        F.lit(0).cast("int").alias("hour"),
        (F.lit(5.0) + F.pmod(key, F.lit(20)) / F.lit(10.0)).alias("temp"),
        F.lit(1).cast("int").alias("_schema_version"),
        F.lit(datetime(2024, 1, 1)).cast("timestamp").alias(
            "_ingestion_timestamp"
        ),
        F.sha2(key.cast("string"), 256).alias("_record_hash"),
    )


def measure_incremental_update(
    spark: SparkSession,
    root: Path,
    *,
    rows: int,
    repeats: int,
) -> dict:
    """Measure duplicate-aware Delta update preparation and insertion."""
    previous_count = rows
    new_count = max(1, rows // 10)
    duplicate_count = max(1, rows // 100)
    previous_path = root / "incremental" / "previous"
    write_delta(synthetic_weather_rows(spark, previous_count), str(previous_path))
    previous = read_delta(spark, str(previous_path))
    incoming = synthetic_weather_rows(
        spark,
        new_count,
        offset=previous_count,
    ).unionByName(previous.orderBy("year").limit(duplicate_count))

    samples = []
    observed = None
    for run in range(1, repeats + 1):
        started = time.perf_counter()
        accepted, rejected = split_novel_records(
            incoming,
            previous,
            "weather_hourly",
        )
        accepted = accepted.persist(StorageLevel.MEMORY_AND_DISK)
        rejected = rejected.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            accepted_count = accepted.count()
            rejected_count = rejected.count()
            write_delta(
                accepted,
                str(root / "incremental" / "accepted"),
            )
        finally:
            accepted.unpersist(blocking=True)
            rejected.unpersist(blocking=True)
        elapsed = time.perf_counter() - started
        samples.append(elapsed)
        current = (accepted_count, rejected_count)
        if observed is not None and current != observed:
            raise AssertionError("Incremental benchmark counts changed between runs")
        observed = current

    if observed != (new_count, duplicate_count):
        raise AssertionError(
            f"Unexpected incremental result: expected {(new_count, duplicate_count)}, "
            f"observed {observed}"
        )
    return {
        "previous_records": previous_count,
        "incoming_records": new_count + duplicate_count,
        "inserted_records": observed[0],
        "duplicate_records": observed[1],
        "samples_seconds": samples,
        "median_seconds": median_seconds(samples),
        "scope": "duplicate-aware raw Delta insertion; downstream refresh measured separately",
    }


def synthetic_integrated_rows(spark: SparkSession, count: int) -> DataFrame:
    timestamp = F.expr(
        "CASE WHEN id % 2 = 0 "
        "THEN TIMESTAMP_NTZ '2024-01-15 00:00:00' "
        "ELSE TIMESTAMP_NTZ '2024-02-15 00:00:00' END "
        "+ pmod(id, 86400) * INTERVAL 1 SECOND"
    )
    frame = spark.range(count).withColumn("pickup_timestamp", timestamp)
    return frame.select(
        F.concat(F.lit("trip-"), F.col("id")).alias("trip_id"),
        "pickup_timestamp",
        F.to_date("pickup_timestamp").alias("pickup_date"),
        F.year("pickup_timestamp").alias("pickup_year"),
        F.month("pickup_timestamp").alias("pickup_month"),
        (F.pmod("id", F.lit(20)) + 1).cast("int").alias(
            "pickup_location_id"
        ),
        F.concat(F.lit("Zone "), F.pmod("id", F.lit(20))).alias("pickup_zone"),
        F.when(F.pmod("id", F.lit(2)) == 0, "Manhattan")
        .otherwise("Queens")
        .alias("pickup_borough"),
        (F.pmod("id", F.lit(4)) + 1).cast("double").alias("passenger_count"),
        (F.pmod("id", F.lit(150)) / F.lit(10.0) + 0.1).alias(
            "trip_distance"
        ),
        (F.pmod("id", F.lit(2400)) + 60).cast("long").alias(
            "trip_duration_seconds"
        ),
        (F.pmod("id", F.lit(5000)) / F.lit(100.0) + 3.0).alias("fare_amount"),
        (F.pmod("id", F.lit(6000)) / F.lit(100.0) + 4.0).alias("total_amount"),
        F.pmod("id", F.lit(10)).cast("int").alias("weather_condition_code"),
        (F.pmod("id", F.lit(500)) / F.lit(10.0)).alias("pm25_avg_ug_m3"),
        F.lit(1).cast("int").alias("taxi_schema_version"),
        F.lit(1).cast("int").alias("pickup_zone_schema_version"),
        F.lit(1).cast("int").alias("dropoff_zone_schema_version"),
        F.lit(2).cast("int").alias("weather_schema_version"),
        F.array(F.lit(1), F.lit(2)).cast("array<int>").alias(
            "air_quality_schema_versions"
        ),
    )


def measure_analytical_refresh(
    spark: SparkSession,
    root: Path,
    *,
    rows: int,
    repeats: int,
) -> dict:
    """Measure scoped refresh of every analytical product."""
    integrated = synthetic_integrated_rows(spark, rows).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    integrated.count()
    scope = integrated.filter("pickup_month = 2")
    scope_records = scope.count()
    samples = []
    per_product: dict[str, list[float]] = {name: [] for name in PRODUCT_BUILDERS}
    row_counts: dict[str, int] = {}
    try:
        for _run in range(1, repeats + 1):
            batch_started = time.perf_counter()
            for name, builder in PRODUCT_BUILDERS.items():
                started = time.perf_counter()
                product = builder(scope).persist(StorageLevel.MEMORY_AND_DISK)
                try:
                    row_count = product.count()
                    write_delta(product, str(root / "products" / name))
                finally:
                    product.unpersist(blocking=True)
                per_product[name].append(time.perf_counter() - started)
                if name in row_counts and row_counts[name] != row_count:
                    raise AssertionError(f"Product row count changed for {name}")
                row_counts[name] = row_count
            samples.append(time.perf_counter() - batch_started)
    finally:
        integrated.unpersist(blocking=True)

    return {
        "source_records": rows,
        "affected_scope_records": scope_records,
        "samples_seconds": samples,
        "median_seconds": median_seconds(samples),
        "product_median_seconds": {
            name: median_seconds(values) for name, values in per_product.items()
        },
        "product_rows": row_counts,
        "scope": "all four products refreshed for one affected month",
    }


def _validation_action(frame: DataFrame, rules) -> tuple[int, int]:
    accepted, rejected = classify_records(frame, "evaluation", rules)
    return accepted.count(), rejected.count()


def measure_validation_overhead(
    spark: SparkSession,
    config: dict,
    *,
    rows: int,
    repeats: int,
) -> dict:
    """Compare the same split with no rules and with representative rules."""
    frame = (
        spark.range(rows)
        .select(
            F.when(F.pmod("id", F.lit(997)) == 0, F.lit(None))
            .otherwise(F.col("id").cast("string"))
            .alias("record_id"),
            F.when(F.pmod("id", F.lit(499)) == 0, F.lit(" "))
            .otherwise(F.lit("complete"))
            .alias("category"),
            F.when(F.pmod("id", F.lit(331)) == 0, F.lit(1000.0))
            .otherwise(F.pmod("id", F.lit(100)).cast("double"))
            .alias("amount"),
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    frame.count()
    rules = [
        required_rule("record_id"),
        non_empty_text_rule("category"),
        range_rule("amount", 0, 100),
    ]
    baseline_samples = []
    enabled_samples = []
    observed = None
    try:
        for _run in range(repeats):
            started = time.perf_counter()
            _validation_action(frame, [])
            baseline_samples.append(time.perf_counter() - started)

            started = time.perf_counter()
            current = _validation_action(frame, rules)
            enabled_samples.append(time.perf_counter() - started)
            if observed is not None and current != observed:
                raise AssertionError("Validation counts changed between runs")
            observed = current

        _, rejected = classify_records(frame, "cleaning", rules)
        write_rejected_records(
            rejected,
            config,
            stage="cleaning",
            dataset_name="weather_hourly",
            mode="overwrite",
        )
        build_validation_report(spark, config)
    finally:
        frame.unpersist(blocking=True)
    return {
        **overhead_metrics(baseline_samples, enabled_samples),
        "records": rows,
        "accepted_records": observed[0],
        "rejected_records": observed[1],
        "rules": 3,
        "method": "same accepted/rejected split with zero rules versus three rules",
    }


def measure_monitoring_overhead(
    spark: SparkSession,
    config: dict,
    *,
    rows: int,
    repeats: int,
) -> dict:
    """Measure one business action with and without a monitoring Delta append."""
    workload = spark.range(rows).withColumn("value", F.col("id") * 2).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    workload.count()
    disabled = deepcopy(config)
    disabled["monitoring"]["enabled"] = False
    baseline_samples = []
    enabled_samples = []

    def business_action() -> None:
        workload.agg(F.sum("value")).first()

    try:
        for run in range(repeats):
            started = time.perf_counter()
            business_action()
            record_operation(
                spark,
                disabled,
                operation_type="evaluation",
                pipeline_step="task5",
                status="SUCCESS",
                started_at=utc_now(),
                finished_at=utc_now(),
                duration_seconds=0.0,
                run_id=f"disabled-{run}-{uuid.uuid4()}",
            )
            baseline_samples.append(time.perf_counter() - started)

            started = time.perf_counter()
            business_action()
            record_operation(
                spark,
                config,
                operation_type="evaluation",
                pipeline_step="task5",
                status="SUCCESS",
                started_at=utc_now(),
                finished_at=utc_now(),
                duration_seconds=0.0,
                run_id=f"enabled-{run}-{uuid.uuid4()}",
                processed_records=rows,
                details={"benchmark": "monitoring_overhead"},
            )
            enabled_samples.append(time.perf_counter() - started)
    finally:
        workload.unpersist(blocking=True)
    return {
        **overhead_metrics(baseline_samples, enabled_samples),
        "records": rows,
        "monitoring_rows_written": repeats,
        "method": "same cached aggregation, with or without one Delta monitoring append",
    }


def path_size(path: Path) -> dict:
    if not path.exists():
        return {"bytes": 0, "files": 0}
    files = [item for item in path.rglob("*") if item.is_file()]
    return {
        "bytes": sum(item.stat().st_size for item in files),
        "files": len(files),
    }


def paths_size(paths) -> dict:
    unique = {Path(path).resolve() for path in paths}
    metrics = [path_size(path) for path in unique]
    return {
        "bytes": sum(item["bytes"] for item in metrics),
        "files": sum(item["files"] for item in metrics),
        "paths": [str(path) for path in sorted(unique)],
    }


def configured_storage_snapshot(config: dict) -> dict:
    """Measure physical files currently used by core and support components."""
    datasets = config["datasets"].values()
    core_paths = [
        table_path(config, dataset[key])
        for dataset in datasets
        for key in ("raw_table", "normal_table")
    ]
    core_paths.append(
        table_path(config, config["data_integration"]["integrated_taxi_trips_table"])
    )
    core_paths.extend(
        table_path(config, relative)
        for relative in config["data_aggregation"].values()
    )
    product_paths = [
        table_path(config, definition["table"])
        for definition in config["data_analysis"]["products"]["definitions"].values()
    ]
    product_paths.append(
        table_path(config, config["data_analysis"]["products"]["catalog_table"])
    )
    quality_paths = [
        table_path(config, config["data_quality"]["rejected_table_root"]),
        table_path(config, config["data_quality"]["validation_summary_table"]),
    ]
    monitoring_paths = [
        table_path(config, config["monitoring"][key])
        for key in ("pipeline_runs_table", "schema_events_table")
    ]
    metadata_root = config.get("paths", {}).get("metadata", "data/metadata")
    result = {
        "core_tables": paths_size(core_paths),
        "analytical_products": paths_size(product_paths),
        "validation": paths_size(quality_paths),
        "monitoring": paths_size(monitoring_paths),
        "metadata_and_update_state": paths_size([metadata_root]),
    }
    overhead = sum(
        result[name]["bytes"]
        for name in (
            "analytical_products",
            "validation",
            "monitoring",
            "metadata_and_update_state",
        )
    )
    core = result["core_tables"]["bytes"]
    result["supporting_overhead_bytes"] = overhead
    result["overhead_percent_of_core"] = (overhead / core * 100) if core else None
    return result


def benchmark_storage_snapshot(root: Path) -> dict:
    categories = {
        "incremental_baseline": root / "incremental" / "previous",
        "incremental_output": root / "incremental" / "accepted",
        "analytical_products": root / "products",
        "validation": root / "lakehouse" / "rejected",
        "validation_summary": root / "lakehouse" / "validation",
        "monitoring": root / "lakehouse" / "monitoring",
    }
    return {name: path_size(path) for name, path in categories.items()}


def latest_release_metrics(config: dict) -> dict | None:
    root = Path(config["incremental"]["state_root"])
    states = []
    for path in root.glob("*/state.json"):
        state = json.loads(path.read_text(encoding="utf-8"))
        states.append((path.stat().st_mtime_ns, path, state))
    if not states:
        return None
    _, path, state = max(states, key=lambda item: item[0])
    return {
        "state_file": str(path),
        "release_id": state.get("release_id"),
        "status": state.get("status"),
        "incremental_update_seconds": state.get("last_attempt_seconds"),
        "integration_refresh_seconds": state.get("integration_refresh_seconds"),
        "aggregate_refresh_seconds": state.get("aggregate_refresh_seconds"),
        "product_refresh_seconds": state.get("product_refresh_seconds", {}),
        "analytical_refresh_seconds": state.get("analytical_refresh_seconds"),
    }


def environment_metadata(spark: SparkSession) -> dict:
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        commit = None
    return {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "spark_version": spark.version,
        "spark_master": spark.sparkContext.master,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "git_commit": commit,
        "timing_method": "wall-clock timings include Spark actions and Delta writes",
        "cache_note": "OS filesystem cache is not cleared; results are warmed local measurements",
    }


def flat_measurements(results: dict) -> list[dict]:
    rows = [
        {
            "metric": "synthetic_incremental_update",
            "seconds": results["incremental_update"]["median_seconds"],
            "percent": None,
        },
        {
            "metric": "synthetic_analytical_refresh",
            "seconds": results["analytical_refresh"]["median_seconds"],
            "percent": None,
        },
        {
            "metric": "validation_overhead",
            "seconds": results["validation_overhead"]["overhead_seconds"],
            "percent": results["validation_overhead"]["overhead_percent"],
        },
        {
            "metric": "monitoring_overhead",
            "seconds": results["monitoring_overhead"]["overhead_seconds"],
            "percent": results["monitoring_overhead"]["overhead_percent"],
        },
    ]
    actual = results.get("latest_release")
    if actual and actual.get("incremental_update_seconds") is not None:
        rows.append(
            {
                "metric": "latest_actual_incremental_update",
                "seconds": actual["incremental_update_seconds"],
                "percent": None,
            }
        )
    if actual and actual.get("analytical_refresh_seconds") is not None:
        rows.append(
            {
                "metric": "latest_actual_analytical_refresh",
                "seconds": actual["analytical_refresh_seconds"],
                "percent": None,
            }
        )
    return rows


def _seconds(value) -> str:
    return "not available" if value is None else f"{value:.4f} s"


def report_markdown(results: dict) -> str:
    incremental = results["incremental_update"]
    analytical = results["analytical_refresh"]
    validation = results["validation_overhead"]
    monitoring = results["monitoring_overhead"]
    storage = results["platform_storage"]
    latest = results.get("latest_release")
    lines = [
        "# Week 3 Task 5 — Production Readiness Evaluation",
        "",
        "## Scope and method",
        "",
        f"This run used Spark {results['environment']['spark_version']} on "
        f"`{results['environment']['spark_master']}` with "
        f"{results['settings']['rows']:,} synthetic base records and "
        f"{results['settings']['repeats']} measured repetitions. Timings are local, "
        "warmed-access wall-clock measurements; the operating-system cache was not cleared. "
        "The suite writes only below its unique evaluation directory.",
        "",
        "The synthetic experiment makes component overhead reproducible and avoids rewriting "
        "the municipality tables. If a completed real release state is available, its separately "
        "instrumented timings are shown below and should be preferred for workload conclusions.",
        "",
        "## Results",
        "",
        "| Measurement | Result | Scope |",
        "|---|---:|---|",
        f"| Incremental update median | {incremental['median_seconds']:.4f} s | "
        f"{incremental['incoming_records']:,} incoming; {incremental['inserted_records']:,} "
        f"inserted; {incremental['duplicate_records']:,} quarantined duplicates |",
        f"| Analytical refresh median | {analytical['median_seconds']:.4f} s | "
        f"Four products over {analytical['affected_scope_records']:,} affected rows |",
        f"| Validation overhead | {validation['overhead_seconds']:.4f} s "
        f"({validation['overhead_percent']:.1f}%) | Three rules over "
        f"{validation['records']:,} rows |",
        f"| Monitoring overhead | {monitoring['overhead_seconds']:.4f} s "
        f"({monitoring['overhead_percent']:.1f}%) | One Delta metadata append per operation |",
    ]
    if latest:
        lines.extend(
            [
                f"| Latest real release update | {_seconds(latest.get('incremental_update_seconds'))} | "
                f"Release `{latest.get('release_id')}`; status `{latest.get('status')}` |",
                f"| Latest real analytical refresh | {_seconds(latest.get('analytical_refresh_seconds'))} | "
                "Aggregate plus affected-product refresh |",
            ]
        )
    else:
        lines.append(
            "| Latest real release | not available | Run an incremental release, then rerun this suite |"
        )
    lines += [
        "",
        "Per-product scoped refresh medians:",
        "",
        "| Product | Median seconds | Output rows |",
        "|---|---:|---:|",
    ]
    for name in PRODUCT_BUILDERS:
        lines.append(
            f"| `{name}` | {analytical['product_median_seconds'][name]:.4f} | "
            f"{analytical['product_rows'][name]:,} |"
        )
    lines += [
        "",
        "## Storage overhead",
        "",
        "Physical bytes include active data, retained Delta history, and transaction logs. "
        "This is a filesystem snapshot, not only active Parquet bytes.",
        "",
        "| Category | Bytes | Files |",
        "|---|---:|---:|",
    ]
    for name in (
        "core_tables",
        "analytical_products",
        "validation",
        "monitoring",
        "metadata_and_update_state",
    ):
        lines.append(
            f"| {name.replace('_', ' ')} | {storage[name]['bytes']:,} | "
            f"{storage[name]['files']:,} |"
        )
    overhead_pct = storage["overhead_percent_of_core"]
    pct_text = "not available" if overhead_pct is None else f"{overhead_pct:.3f}%"
    lines += [
        "",
        f"Supporting tables, products, and metadata occupy "
        f"**{storage['supporting_overhead_bytes']:,} bytes**, or **{pct_text}** of the "
        "configured core-table footprint at measurement time.",
        "",
        "## Production-readiness assessment",
        "",
        "Week 1 choices that simplified maintenance were immutable raw data, layered tables, "
        "central YAML contracts, shared Delta I/O helpers, and source lineage. They allowed "
        "incremental processing, monitoring, and validation to be added without rewriting the "
        "analytical SQL contract.",
        "",
        "The largest modifications were the release state machine and dependency-aware refresh. "
        "They must coordinate idempotent writes, schema versions, affected scopes, retries, and "
        "cross-layer consistency. Validation was smaller because accepted/rejected splitting was "
        "already present and could be generalized into rule objects.",
        "",
        "Future datasets are supported well when they can declare a source schema, business key, "
        "cleaner, and dependency mapping. The remaining coupling is the fixed dictionaries for "
        "raw/normal keys and product dependencies. A redesign would move those declarations into "
        "a typed dataset registry, use an external transaction/orchestration service for multi-table "
        "commits, and export metrics to a time-series alerting system rather than relying only on "
        "local Delta queries.",
        "",
        "## Interpretation and limitations",
        "",
        "Validation cost depends on rule complexity and rejection rate; reference joins should be "
        "measured separately when lookup tables stop fitting in broadcast memory. Monitoring cost is "
        "dominated by small Delta commits, so batching records would reduce overhead at higher run "
        "frequency. Local synthetic timings demonstrate the method and relative component cost, not "
        "cluster-scale throughput or statistical significance. Rerun on the deployment hardware and "
        "retain multiple real releases before setting service-level objectives.",
        "",
        "Artifacts: `measurements.json`, `measurements.csv`, and the isolated `artifacts/` Delta "
        "tables. All counts are checked across repetitions before a result is reported.",
        "",
    ]
    return "\n".join(lines)


def run_evaluation(
    spark: SparkSession,
    config: dict,
    output_root: Path,
    *,
    rows: int,
    repeats: int,
    export_report: Path | None = None,
) -> Path:
    if rows < 100 or repeats < 1:
        raise ValueError("rows must be at least 100 and repeats must be positive")
    suite_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    root = Path(output_root) / suite_id
    root.mkdir(parents=True, exist_ok=False)
    artifacts = root / "artifacts"
    isolated = deepcopy(config)
    isolated["paths"]["lakehouse"] = str(artifacts / "lakehouse")
    isolated["monitoring"]["enabled"] = True

    results = {
        "status": "running",
        "suite_id": suite_id,
        "settings": {"rows": rows, "repeats": repeats},
        "environment": environment_metadata(spark),
    }
    write_json(root / "measurements.json", results)
    try:
        results["incremental_update"] = measure_incremental_update(
            spark,
            artifacts,
            rows=rows,
            repeats=repeats,
        )
        results["analytical_refresh"] = measure_analytical_refresh(
            spark,
            artifacts,
            rows=rows,
            repeats=repeats,
        )
        results["validation_overhead"] = measure_validation_overhead(
            spark,
            isolated,
            rows=rows,
            repeats=repeats,
        )
        results["monitoring_overhead"] = measure_monitoring_overhead(
            spark,
            isolated,
            rows=max(100, rows // 10),
            repeats=repeats,
        )
        results["benchmark_storage"] = benchmark_storage_snapshot(artifacts)
        results["platform_storage"] = configured_storage_snapshot(config)
        results["latest_release"] = latest_release_metrics(config)
        results["status"] = "completed"
        write_json(root / "measurements.json", results)
        write_csv(root / "measurements.csv", flat_measurements(results))
        report = report_markdown(results)
        (root / "evaluation_report.md").write_text(report, encoding="utf-8")
        if export_report is not None:
            export_report.parent.mkdir(parents=True, exist_ok=True)
            export_report.write_text(report, encoding="utf-8")
        print(f"PRODUCTION_EVALUATION_COMPLETED={root}", flush=True)
        return root
    except Exception as error:
        results.update({"status": "failed", "error": str(error)})
        write_json(root / "measurements.json", results)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/config.yaml"))
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--rows", type=int)
    parser.add_argument("--repeats", type=int)
    parser.add_argument("--export-report", type=Path)
    args = parser.parse_args()
    config = load_config(args.config)
    settings = config.get("evaluation", {})
    output_root = args.output_root or Path(
        settings.get("results_root", "data/evaluation/week3")
    )
    rows = args.rows or int(settings.get("synthetic_rows", 10_000))
    repeats = args.repeats or int(settings.get("repeats", 3))
    spark = create_spark()
    try:
        run_evaluation(
            spark,
            config,
            output_root,
            rows=rows,
            repeats=repeats,
            export_report=args.export_report,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
