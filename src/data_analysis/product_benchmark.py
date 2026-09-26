"""Benchmark Task 4 materialized products against equivalent on-demand builds."""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import random
import statistics
import time
import uuid

from src.common import create_spark, load_config, table_path
from src.data_analysis.data_products import PRODUCT_BUILDERS
from src.data_analysis.execution_metrics import executed_metrics, scan_summary
from src.data_analysis.product_metadata import delta_snapshot, load_catalog_records
from src.data_analysis.result_validation import ResultSnapshot, compare_results


def write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, default=str), encoding="utf-8")


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        raise ValueError(f"No rows for {path}")
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def summarize(timings: list[dict], catalog: dict[str, dict], blocks: int) -> list[dict]:
    rows = []
    for product in PRODUCT_BUILDERS:
        samples = {
            variant: [r["seconds"] for r in timings
                      if r["product_name"] == product and r["variant"] == variant]
            for variant in ("on_demand", "materialized")
        }
        block_medians = {
            variant: {
                block: statistics.median(r["seconds"] for r in timings
                                         if r["product_name"] == product
                                         and r["variant"] == variant and r["block"] == block)
                for block in range(1, blocks + 1)
            }
            for variant in samples
        }
        source_median = statistics.median(samples["on_demand"])
        product_median = statistics.median(samples["materialized"])
        record = catalog[product]
        rows.append({
            "product_name": product,
            "samples_per_variant": len(samples["on_demand"]),
            "on_demand_median_seconds": source_median,
            "materialized_median_seconds": product_median,
            "speedup": source_median / product_median,
            "materialized_block_wins": sum(
                block_medians["materialized"][block] < block_medians["on_demand"][block]
                for block in range(1, blocks + 1)
            ),
            "blocks": blocks,
            "row_count": record["row_count"],
            "storage_bytes": record["storage_bytes"],
            "refresh_duration_seconds": record["refresh_duration_seconds"],
            "source_delta_version": record["source_delta_version"],
        })
    return rows


def report_markdown(summary: list[dict], metadata: dict, evidence: list[dict]) -> str:
    total_storage = sum(r["storage_bytes"] for r in summary)
    total_refresh = sum(r["refresh_duration_seconds"] for r in summary)
    lines = [
        "# Task 5 Product Materialization Benchmark",
        "",
        "## Method",
        "",
        f"Suite `{metadata['suite_id']}` used Spark {metadata['spark_version']} on "
        f"`{metadata['spark_master']}`. Each arm ran {metadata['blocks']} blocks with "
        f"one warm-up and {metadata['measured_runs']} timed `collect()` actions per block. "
        "Pair order was seeded and shuffled. Both arms used the product catalog's pinned "
        "Integrated Delta source version. Timings are warmed-access wall-clock measurements; "
        "the OS file cache was not cleared. Product refresh, correctness checks, plan export, "
        "and input setup were outside query latency.",
        "",
        "For each product, the on-demand arm executes the same transformation used to build "
        "the product; the materialized arm reads the active product Delta snapshot and selects "
        "the same analytical columns. Every timed result was checked for matching ordered names/"
        "types, duplicate-preserving rows, and floating-point tolerance (atol 1e-8, rtol 1e-6).",
        "",
        "## Results",
        "",
        "| Product | On demand s | Materialized s | Speedup | Block wins | Rows | Storage bytes | Refresh s | Equal |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in summary:
        lines.append(
            f"| {row['product_name']} | {row['on_demand_median_seconds']:.4f} | "
            f"{row['materialized_median_seconds']:.4f} | {row['speedup']:.2f}x | "
            f"{row['materialized_block_wins']}/{row['blocks']} | {row['row_count']} | "
            f"{row['storage_bytes']:,} | {row['refresh_duration_seconds']:.3f} | True |"
        )
    lines += [
        "",
        f"The four active product snapshots occupy **{total_storage:,} bytes** in total. "
        f"Their separately measured full-refresh durations sum to **{total_refresh:.3f} seconds**; "
        "these products were refreshed sequentially, so the sum is the relevant observed batch cost. "
        "The catalog Delta table and benchmark artifacts are not included in product-table storage.",
        "",
        "## Physical-plan evidence",
        "",
        "| Product | Arm | Selected files | Selected-file bytes | Plan |",
        "|---|---|---:|---:|---|",
    ]
    for row in evidence:
        scans = row["scans"]
        lines.append(
            f"| {row['product_name']} | {row['variant']} | "
            f"{sum(s['files'] or 0 for s in scans)} | {sum(s['file_bytes'] or 0 for s in scans):,} | "
            f"[{row['variant']}](plans/{row['product_name']}_{row['variant']}_final.txt) |"
        )
    lines += [
        "",
        "Selected-file bytes are whole-file scan metrics, not physical bytes read after column pruning. "
        "The materialized plans replace fact-level aggregation (and, for daily mobility, repeated fact "
        "branches/window work) with a small product scan. Products are deliberately unpartitioned because "
        "their current row/file counts are small; partitioning them would risk tiny-file overhead.",
        "",
        "## Trade-offs",
        "",
        "Materialization exchanges refresh latency and storage for lower repeated-query latency and a "
        "stable metric contract. Results can be stale between refreshes, and the four overwrites plus "
        "catalog update are not one cross-table transaction. At ten-city scale, use incremental refresh, "
        "include city/time-zone identity, monitor product growth before choosing city/date partitions, "
        "and rerun these measurements on the target cluster rather than extrapolating local speedups.",
        "",
        "Files: `summary.csv`, `timings.csv`, `validations.csv`, `evidence.json`, `metadata.json`, "
        "and `plans/`. No statistical-significance claim is made from three blocks.",
        "",
    ]
    return "\n".join(lines)


def run_product_benchmark(spark, config: dict, output_root: Path, *, blocks=3,
                          warmup_runs=1, measured_runs=5, seed=2221) -> Path:
    if min(blocks, measured_runs) < 1 or warmup_runs < 0:
        raise ValueError("blocks/measured_runs must be positive and warmup_runs non-negative")
    catalog = load_catalog_records(spark, config)
    missing = sorted(set(PRODUCT_BUILDERS) - set(catalog))
    if missing:
        raise ValueError(f"Build all Task 4 products before benchmarking; missing {missing}")
    source_versions = {int(catalog[name]["source_delta_version"]) for name in PRODUCT_BUILDERS}
    if len(source_versions) != 1:
        raise ValueError("Products do not share one pinned source version; refresh them together")
    source_version = source_versions.pop()
    source_path = table_path(config, next(iter(catalog.values()))["source_table"])
    integrated = spark.read.format("delta").option("versionAsOf", source_version).load(source_path)

    suite_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    root = Path(output_root) / suite_id
    root.mkdir(parents=True, exist_ok=False)
    metadata = {
        "suite_id": suite_id, "status": "running", "created_at_utc": datetime.now(timezone.utc),
        "spark_version": spark.version, "spark_master": spark.sparkContext.master,
        "blocks": blocks, "warmup_runs": warmup_runs, "measured_runs": measured_runs,
        "seed": seed, "source_path": source_path, "source_delta_version": source_version,
        "method": "warmed access; timed dataframe construction plus collect; OS cache not cleared",
    }
    write_json(root / "metadata.json", metadata)
    rng = random.Random(seed)
    timings, validations, evidence = [], [], []
    references = {}
    makers = {}

    try:
        for name, builder in PRODUCT_BUILDERS.items():
            expected_df = builder(integrated)
            columns = expected_df.columns
            product_path = table_path(config, catalog[name]["table_path"])
            product_state = delta_snapshot(spark, product_path)
            if int(catalog[name]["row_count"]) <= 0:
                raise ValueError(f"Product {name} is empty")
            makers[(name, "on_demand")] = lambda b=builder: b(integrated)
            makers[(name, "materialized")] = lambda p=product_path, v=product_state["version"], c=columns: (
                spark.read.format("delta").option("versionAsOf", v).load(p).select(*c)
            )
            reference = ResultSnapshot.build(expected_df.schema, expected_df.collect())
            product_reference_df = makers[(name, "materialized")]()
            product_reference = ResultSnapshot.build(product_reference_df.schema, product_reference_df.collect())
            check = compare_results(reference, product_reference)
            if not check["results_equal"]:
                raise AssertionError(f"Existing product {name} does not match pinned source: {check}")
            references[name] = reference
            metadata.setdefault("products", {})[name] = {
                "product_path": product_path, "product_delta_version": product_state["version"],
                "active_files": product_state["active_files"], "active_bytes": product_state["size_bytes"],
                "row_count": catalog[name]["row_count"],
                "refresh_duration_seconds": catalog[name]["refresh_duration_seconds"],
            }

        pairs = list(makers)
        for block in range(1, blocks + 1):
            order = list(pairs)
            rng.shuffle(order)
            metadata.setdefault("block_order", {})[str(block)] = [f"{p}:{v}" for p, v in order]
            for name, variant in order:
                for _ in range(warmup_runs):
                    makers[(name, variant)]().collect()
                initial = makers[(name, variant)]()
                if block == 1:
                    plan = spark._jvm.PythonSQLUtils.explainString(initial._jdf.queryExecution(), "formatted")
                    plan_path = root / "plans" / f"{name}_{variant}_initial.txt"
                    plan_path.parent.mkdir(parents=True, exist_ok=True)
                    plan_path.write_text(plan, encoding="utf-8")
                last_df = None
                for run in range(1, measured_runs + 1):
                    started = time.perf_counter()
                    dataframe = makers[(name, variant)]()
                    rows = dataframe.collect()
                    elapsed = time.perf_counter() - started
                    actual = ResultSnapshot.build(dataframe.schema, rows)
                    check = compare_results(references[name], actual)
                    timings.append({"product_name": name, "variant": variant, "block": block,
                                    "run": run, "seconds": elapsed})
                    validations.append({"product_name": name, "variant": variant, "block": block,
                                        "run": run, **check})
                    if not check["results_equal"]:
                        write_csv(root / "validations.csv", validations)
                        raise AssertionError(f"Validation failed for {name}/{variant}, block {block}, run {run}")
                    last_df = dataframe
                if block == 1:
                    final = spark._jvm.PythonSQLUtils.explainString(last_df._jdf.queryExecution(), "formatted")
                    (root / "plans" / f"{name}_{variant}_final.txt").write_text(final, encoding="utf-8")
                    payload = executed_metrics(last_df)
                    write_json(root / "metrics" / f"{name}_{variant}.json", payload)
                    evidence.append({"product_name": name, "variant": variant,
                                     "scans": scan_summary(payload)})
        summary = summarize(timings, catalog, blocks)
        write_csv(root / "timings.csv", timings)
        write_csv(root / "validations.csv", validations)
        write_csv(root / "summary.csv", summary)
        write_json(root / "evidence.json", evidence)
        metadata.update({"status": "completed", "timed_actions": len(timings),
                         "validation_checks": len(validations),
                         "all_results_equal": all(r["results_equal"] for r in validations)})
        write_json(root / "metadata.json", metadata)
        (root / "benchmark_report.md").write_text(
            report_markdown(summary, metadata, evidence), encoding="utf-8")
        print(f"PRODUCT_BENCHMARK_COMPLETED={root}")
        return root
    except Exception as error:
        metadata.update({"status": "failed", "error": str(error)})
        write_json(root / "metadata.json", metadata)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/config.yaml"))
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--blocks", type=int, default=3)
    args = parser.parse_args()
    config = load_config(args.config)
    root = args.output_root or Path(table_path(config, "analysis/product_benchmarks"))
    spark = create_spark()
    try:
        run_product_benchmark(spark, config, root, blocks=args.blocks)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
