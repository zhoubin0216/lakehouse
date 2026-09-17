"""Reproduce Task 3/5 experiments, stopping before Task 4 products."""
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from datetime import datetime, timezone
import json
from pathlib import Path
import random
import shutil
import statistics
import uuid

from src.common import create_spark, load_config, table_path
from src.data_analysis.execution_metrics import scan_summary
from src.data_analysis.optimization_benchmark import (
    VARIANTS, Variant, environment, prepare_inputs, run_benchmark, write_csv, write_json,
)
from src.data_analysis.optimization_queries import pruning_queries
from src.data_analysis.query_library import ANALYTICAL_QUERIES

JOIN_QUERIES = {"air_quality_demand_relationship", "zone_weather_variation", "monthly_demand_trend"}


def read_csv(path):
    with Path(path).open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def aggregate(jobs, suite_root):
    raw, validations, grouped, block_medians = [], [], defaultdict(list), defaultdict(dict)
    for job in jobs:
        root = suite_root / job["path"]
        for row in read_csv(root / "timings.csv"):
            record = {"experiment": job["experiment"], "block": job["block"], **row}
            raw.append(record)
            grouped[(job["experiment"], row["query_name"], row["variant"])].append(float(row["seconds"]))
        for row in read_csv(root / "benchmark_results.csv"):
            key = (job["experiment"], row["query_name"], row["variant"])
            block_medians[key][job["block"]] = float(row["median_seconds"])
        for row in read_csv(root / "validation_results.csv"):
            validations.append({"experiment": job["experiment"], "block": job["block"], **row})
    summaries = []
    for (experiment, query, variant), samples in grouped.items():
        key = (experiment, query, variant)
        baseline_key = (experiment, query, "baseline")
        base_samples = grouped[baseline_key]
        median, base = statistics.median(samples), statistics.median(base_samples)
        wins = sum(value < block_medians[baseline_key][block]
                   for block, value in block_medians[key].items())
        summaries.append({"experiment": experiment, "query_name": query, "variant": variant,
                          "samples": len(samples), "blocks": len(block_medians[key]),
                          "median_seconds": median, "min_seconds": min(samples), "max_seconds": max(samples),
                          "baseline_median_seconds": base, "speedup": base / median,
                          "block_wins": wins,
                          "results_equal": all(r["results_equal"] == "True" for r in validations
                                               if r["experiment"] == experiment and r["query_name"] == query
                                               and r["variant"] == variant)})
    return summaries, raw, validations


def select_candidates(summaries):
    """Predeclared exploratory selection, followed by independent confirmation."""
    candidates = {}
    for name in ANALYTICAL_QUERIES:
        records = [r for r in summaries if r["experiment"] == "full_queries" and r["query_name"] == name]
        eligible = [r for r in records if r["variant"] != "baseline"
                    and (r["variant"] not in ("broadcast", "combined") or name in JOIN_QUERIES)
                    and r["speedup"] >= 1.05 and r["block_wins"] >= (r["blocks"] // 2 + 1)]
        candidates[name] = min(eligible, key=lambda r: r["median_seconds"])["variant"] if eligible else "baseline"
    return candidates


def verify_plan_evidence(jobs, suite_root):
    representative = {}
    for job in jobs:
        if job["block"] != 1 or job["experiment"].startswith("confirmation_"):
            continue
        root = suite_root / job["path"]
        for row in read_csv(root / "benchmark_results.csv"):
            query, variant = row["query_name"], row["variant"]
            plan = (root / row["final_plan"]).read_text(encoding="utf-8")
            metrics = json.loads((root / variant / "metrics" / f"{query}_run_5.json").read_text())
            representative[(job["experiment"], query, variant)] = {
                "scans": scan_summary(metrics), "broadcast_hash_join": "BroadcastHashJoin" in plan,
                "sort_merge_join": "SortMergeJoin" in plan,
                "in_memory_scan": "In-memory" in plan,
                "aqe_final": "isFinalPlan=true" in plan,
                "coalesced_reads": sum("AQEShuffleRead" in n["node"] for n in metrics["nodes"]),
                "adaptive_partition_metrics": [n["metrics"] for n in metrics["nodes"]
                                               if "AQEShuffleRead" in n["node"]],
                "plan": str((root / row["final_plan"]).relative_to(suite_root))}
    q4 = "zone_weather_variation"
    assert not representative[("full_queries", q4, "baseline")]["broadcast_hash_join"]
    assert representative[("full_queries", q4, "broadcast")]["broadcast_hash_join"]
    assert representative[("full_queries", q4, "aqe")]["aqe_final"]
    assert representative[("full_queries", "monthly_zone_demand", "cache")]["in_memory_scan"]
    for query in pruning_queries()[0]:
        scans = representative[("pruning", query, "pruned")]["scans"]
        assert any(s["partition_filters"] and s["partition_filters"] != "[]" for s in scans)
    return [{"experiment": key[0], "query_name": key[1], "variant": key[2], **value}
            for key, value in representative.items()]


def build_report(summaries, final, evidence, metadata, jobs, suite_root):
    full = [r for r in summaries if r["experiment"] == "full_queries"]
    lines = ["# Week 2 Benchmark Report — Tasks 3 and 5 (pre-Task-4)", "",
             "## Scope and methodology", "",
             f"Suite `{metadata['suite_id']}`; {metadata['environment']['spark_version']} Spark, "
             f"Delta {metadata['environment']['delta_version']}, "
             f"{metadata['environment']['spark_master']}, driver {metadata['environment']['driver_memory']}.", "",
             "Six original SQL files are unchanged. Weather codes and Q4 semantics use the current "
             "query contract; teammate confirmation is still pending. This is not a Task 4 implementation.", "",
             f"Each configuration has {metadata['blocks']} blocks, one warm-up and five timed actions per block. "
             "Variants are shuffled with a recorded seed. Timings include SQL construction and collect, "
             "exclude correctness checks, plan export, input setup, and cache construction. Correctness "
             "references are collected before variants, so these are warmed-access tests, NOT cold-disk tests. "
             "OS cache is not cleared; shared-machine background activity is not controlled. Blocks, not "
             "individual repeats, are the experimental units; no statistical-significance claim is made.", "",
             "Baseline disables AQE and automatic/adaptive broadcast and clears Spark table caches. "
             "Both baseline and optimized queries retain Delta column pruning/statistics. Main experiments "
             "use 4 initial shuffle partitions; a separate AQE sensitivity test uses 32 in BOTH arms. "
             "All input Delta versions are pinned for the entire suite. Exact hashes preserve duplicate "
             "rows; schema names/types must match, with float tolerances atol=1e-8 and rtol=1e-6.", "",
             "## Input data", "", "| Input | Analysis rows | Active files | Active bytes | Partitions |",
             "|---|---:|---:|---:|---|"]
    for source in metadata["environment"]["inputs"]:
        lines.append(f"| {source['view']} | {source['analysis_rows']:,} | {source['active_files']} | "
                     f"{source['size_bytes']:,} | {', '.join(source['partition_columns']) or 'none'} |")
    lines += ["", "## Full-quarter, single-technique and combined experiments", "",
              "Times are pooled medians across blocks. Speedup = matched baseline median / variant median. "
              "Broadcast and combined SQL hints only affect Q3/Q4/Q6; broadcast measurements of Q1/Q2/Q5 "
              "are no-op timing controls, not evidence of a broadcast improvement.", "",
              "| Query | Baseline s | Cache s | Broadcast s | AQE s | Combined s |", "|---|---:|---:|---:|---:|---:|"]
    for name in ANALYTICAL_QUERIES:
        values = {r["variant"]: r["median_seconds"] for r in full if r["query_name"] == name}
        lines.append(f"| {name} | " + " | ".join(f"{values[v]:.4f}" for v in VARIANTS) + " |")
    lines += ["", "## Partition pruning: identical single-month questions", "",
              "The function-filter case retains the same date_format predicate in both arms; optimized SQL "
              "adds equivalent year/month partition filters. The range-filter control tests whether Delta "
              "statistics already skip the same files. Year/month consistency against pickup_timestamp "
              "was checked on the complete pinned integrated dataset. Neither arm compares January with "
              "the full quarter as its analytical result.", "",
              "| Case | Baseline s | Pruned s | Speedup | Baseline files | Pruned files |", "|---|---:|---:|---:|---:|---:|"]
    for query in pruning_queries()[0]:
        r = next(r for r in summaries if r["experiment"] == "pruning" and r["query_name"] == query and r["variant"] == "pruned")
        scans = {v: next(e for e in evidence if e["experiment"] == "pruning" and e["query_name"] == query and e["variant"] == v)["scans"]
                 for v in ("baseline", "pruned")}
        counts = {v: sum(s["files"] or 0 for s in items) for v, items in scans.items()}
        lines.append(f"| {query} | {r['baseline_median_seconds']:.4f} | {r['median_seconds']:.4f} | "
                     f"{r['speedup']:.2f}x | {counts['baseline']} | {counts['pruned']} |")
    lines += ["", "Selected-file bytes and counts are runtime scan metrics, not bytes actually read after "
              "column pruning. The range-filter control may show little extra benefit if statistics already "
              "select the same files. Consult plan_evidence.json for observed filters and scan counts.", "",
              "## AQE sensitivity", "", "| Query (32 initial partitions in both arms) | AQE off s | AQE on s | Speedup |",
              "|---|---:|---:|---:|"]
    for r in summaries:
        if r["experiment"] == "aqe_32" and r["variant"] == "aqe":
            lines.append(f"| {r['query_name']} | {r['baseline_median_seconds']:.4f} | {r['median_seconds']:.4f} | {r['speedup']:.2f}x |")
    lines += ["", "This is a sensitivity workload, not a claim that 32 is optimal for this machine. "
              "AQE metrics and final plans report whether coalescing actually happened. Dynamic broadcast "
              "remains disabled, so this comparison does not conflate AQE with broadcast hints.", "",
              "## Independent confirmation of selected configurations", "",
              "Selection requires >=5% exploratory median improvement and wins in a majority of blocks. "
              "Only applicable variants are eligible. Selected configurations are frozen and independently "
              "re-measured; confirmation is reported even if it loses the exploratory improvement. When "
              "no candidate qualifies, the baseline is retained (speedup 1, not an invented optimization).", "",
              "| Query | Selected configuration | Confirmation baseline s | Confirmation selected s | Speedup | Equal |",
              "|---|---|---:|---:|---:|---|"]
    for r in final:
        lines.append(f"| {r['query_name']} | {r['selected_variant']} | {r['baseline_seconds']:.4f} | "
                     f"{r['optimized_seconds']:.4f} | {r['speedup']:.2f}x | {r['results_equal']} |")
    lines += ["", "## Findings supported by the measurements", ""]
    techniques = [r for r in summaries if r["variant"] in ("cache", "aqe", "pruned", "broadcast")
                  and r["experiment"] != "aqe_32"
                  and (r["variant"] != "broadcast" or r["query_name"] in JOIN_QUERIES)]
    best = max(techniques, key=lambda r: r["speedup"])
    lines.append(f"Largest observed relative improvement among individual techniques: **{best['variant']}** "
                 f"for `{best['query_name']}` ({best['experiment']}), **{best['speedup']:.2f}x** "
                 f"({best['baseline_median_seconds']:.4f} → {best['median_seconds']:.4f}s). "
                 "This is workload-specific; selective and full-quarter speedups cannot establish a universal winner.")
    for name in ("cache", "broadcast", "aqe"):
        rows = [r for r in techniques if r["variant"] == name]
        low, high = min(rows, key=lambda r: r["speedup"]), max(rows, key=lambda r: r["speedup"])
        lines += ["", f"{name}: observed range {low['speedup']:.2f}x–{high['speedup']:.2f}x "
                  f"across applicable full-quarter queries; lowest at `{low['query_name']}`. "
                  f"Best case won {high['block_wins']}/{high['blocks']} exploratory blocks. "
                  "Values below 1 are slowdowns; small/mixed changes should not be interpreted as stable wins."]
    cache_costs, memory, disk = [], [], []
    for job in jobs:
        if job["experiment"] != "full_queries":
            continue
        cfg = json.loads((suite_root / job["path"] / "cache/configuration.json").read_text())
        cache_costs.append(cfg["cache_build_seconds"])
        rdds = [r for r in cfg["storage"] if r["name"].startswith("In-memory table integrated")]
        memory.append(sum(r["memory_bytes"] for r in rdds))
        disk.append(sum(r["disk_bytes"] for r in rdds))
    lines += ["", f"Full-cache build median: {statistics.median(cache_costs):.3f}s; "
              f"recorded memory range {min(memory):,}–{max(memory):,} bytes, disk range "
              f"{min(disk):,}–{max(disk):,} bytes. Cache cost is shared across the six-query workload, "
              "not charged six times. A faster steady-state query does not alone justify caching. "
              "Parquet reads only required columns and OS cache is warmed. Cache conversion/decoding and "
              "wide-table processing are plausible explanations for regressions, not causally proven by "
              "these timings; there is no CPU profiler in this experiment."]
    sorted_final = sorted(final, key=lambda r: r["optimized_seconds"], reverse=True)
    lines += ["", "Largest remaining confirmed latencies: " + "; ".join(
        f"`{r['query_name']}` {r['optimized_seconds']:.4f}s" for r in sorted_final[:2]) +
        ". Q4 has two fact-table scan branches, multiple aggregation stages, a zero-demand "
        "zone/weather cross product and joins; plan evidence supports its structural complexity, "
        "but does not prove one operator is the sole bottleneck.", "",
        "Broadcast is appropriate because hourly context/aggregated join inputs are small relative to "
        "9.55 million trips. It removes sort-merge joins where supported, but copies the build side to "
        "each worker and can fail for larger future inputs. AQE adds planning overhead and adjusts "
        "shuffle partitions; its value depends on measured shuffle volume and initial partition count. "
        "Partition pruning reduces selected files for partition-aligned filters but restricts the analysis "
        "period and cannot accelerate an unchanged full-quarter result merely by dropping months.", "",
        "## Storage overhead and ten-city expansion", "",
        "No analytical products or duplicate Delta fact layouts were created by this suite: additional "
        "persistent analytical-table storage is zero at this stage. Benchmark artifact bytes are recorded "
        "separately. Task 4 product storage, refresh cost and on-demand-vs-product latency are **pending** "
        "and are not claimed as measured here.", "",
        "Ten-city changes are recommendations inferred from this one-city experiment, NOT measured "
        "ten-city speedups: add city identity and explicit local-time/UTC contracts; keep date-aligned "
        "pruning and measure city/date file sizes before selecting partition granularity; broadcast only "
        "dimensions verified to remain small; size resources using measured cache footprint rather than "
        "compressed Delta size; repeat AQE/partition-count experiments on the larger cluster; "
        "collect per-stage skew/spill metrics rather than assuming skew; and use incremental Task 4 "
        "product refreshes instead of repeated full-trip scans. Cache scaling to ten equally sized cities "
        "is a capacity scenario, not a runtime extrapolation.", "",
        "## Reproduction and evidence", "", "```bash", ".venv/bin/pytest -q",
        ".venv/bin/python -m src.data_analysis.optimization_suite", "```", "",
        "See aggregate_results.csv, final_query_comparison.csv, all_timings.csv, all_validations.csv, "
        "suite_manifest.json, plan_evidence.json and evidence/ for auditable data. The compact export "
        "contains representative SQL/configuration/initial+final plans/runtime metrics, while every "
        "run's complete artifacts remain in data/lakehouse/analysis/optimization/suites/. "
        "Use the recorded seed, blocks and configuration; do not compare different computers' times.", ""]
    return "\n".join(lines)


def export_evidence(root, destination, jobs):
    """Export a unique, Git-visible report bundle; never overwrite older runs."""
    target = Path(destination) / root.name
    target.mkdir(parents=True, exist_ok=False)
    for name in ("aggregate_results.csv", "final_query_comparison.csv", "all_timings.csv",
                 "all_validations.csv", "suite_manifest.json", "plan_evidence.json", "benchmark_report.md"):
        shutil.copy2(root / name, target / name)
    for job in jobs:
        if job["block"] != 1:
            continue
        source = root / job["path"]
        dest = target / "evidence" / job["experiment"]
        dest.mkdir(parents=True)
        shutil.copy2(source / "environment.json", dest / "environment.json")
        for row in read_csv(source / "benchmark_results.csv"):
            variant, query = row["variant"], row["query_name"]
            vd = dest / variant
            vd.mkdir(exist_ok=True)
            for path in (source / row["initial_plan"], source / row["final_plan"],
                         source / variant / "metrics" / f"{query}_run_5.json",
                         source / variant / "configuration.json", source / variant / f"{query}.sql"):
                shutil.copy2(path, vd / path.name)
    return target


def run_suite(spark, config, output_root, export_root=None, blocks=None):
    settings = dict(config["data_analysis"]["optimization_benchmark"])
    suite_config = settings.get("suite", {})
    blocks = blocks if blocks is not None else suite_config.get("blocks", 3)
    if not isinstance(blocks, int) or blocks < 1:
        raise ValueError("blocks must be positive")
    if settings.get("measured_runs", 5) != 5:
        raise ValueError("Suite uses exactly five measured runs per block")
    if settings.get("warmup_runs", 1) != 1:
        raise ValueError("Suite uses exactly one warm-up per block")
    seed = suite_config.get("seed", 2221)
    rng = random.Random(seed)
    suite_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    root = Path(output_root) / suite_id
    root.mkdir(parents=True, exist_ok=False)
    inputs = prepare_inputs(spark, config)
    mismatches = spark.sql("""SELECT SUM(CASE WHEN
        NOT (pickup_year <=> YEAR(pickup_timestamp)) OR
        NOT (pickup_month <=> MONTH(pickup_timestamp)) THEN 1 ELSE 0 END) AS n
        FROM integrated_taxi_trips""").first().n or 0
    if mismatches:
        raise AssertionError("Partition columns disagree with timestamps; pruning equivalence not valid")
    metadata = {"suite_id": suite_id, "blocks": blocks, "seed": seed,
                "partition_consistency_mismatches": mismatches, "environment": environment(spark, inputs),
                "settings": settings, "jobs": [], "status": "running",
                "products": "pending Task 4; no product tables built or evaluated"}
    write_json(root / "suite_manifest.json", metadata)
    print(f"SUITE_OUTPUT={root}", flush=True)
    jobs = metadata["jobs"]

    def run_job(experiment, block, queries, variants, options=settings):
        shuffled = list(variants)
        rng.shuffle(shuffled)
        path = run_benchmark(spark, queries, root / experiment / f"block_{block}",
                             options, shuffled, inputs)
        jobs.append({"experiment": experiment, "block": block,
                     "variant_order": [v.name for v in shuffled], "path": str(path.relative_to(root))})
        write_json(root / "suite_manifest.json", metadata)

    try:
        for block in range(1, blocks + 1):
            run_job("full_queries", block, ANALYTICAL_QUERIES, list(VARIANTS.values()))
        baseline, pruned = pruning_queries(suite_config.get("year", 2024), suite_config.get("month", 1))
        for block in range(1, blocks + 1):
            run_job("pruning", block, baseline, [VARIANTS["baseline"], Variant("pruned", sql_overrides=pruned)])
        aqe32_settings = {**settings, "spark_config": {**settings.get("spark_config", {}),
                                                     "spark.sql.shuffle.partitions": "32"}}
        for block in range(1, blocks + 1):
            run_job("aqe_32", block,
                    {n: ANALYTICAL_QUERIES[n] for n in ("monthly_zone_demand", "zone_weather_variation")},
                    [VARIANTS["baseline"], VARIANTS["aqe"]], aqe32_settings)
        summaries, _, _ = aggregate(jobs, root)
        selection = select_candidates(summaries)
        metadata["frozen_selection"] = selection
        write_json(root / "suite_manifest.json", metadata)
        for block in range(1, blocks + 1):
            for query, candidate in selection.items():
                variants = [VARIANTS["baseline"]]
                if candidate != "baseline":
                    chosen = VARIANTS[candidate]
                    variants.append(Variant("selected", chosen.spark_config, chosen.cache_views,
                                            {query: chosen.sql_overrides.get(query, ANALYTICAL_QUERIES[query])}))
                run_job(f"confirmation_{query}", block, {query: ANALYTICAL_QUERIES[query]}, variants)
        summaries, timings, validations = aggregate(jobs, root)
        evidence = verify_plan_evidence(jobs, root)
        final = []
        for query, candidate in selection.items():
            row = next(r for r in summaries if r["experiment"] == f"confirmation_{query}"
                       and r["variant"] == ("baseline" if candidate == "baseline" else "selected"))
            final.append({"query_name": query, "selected_variant": candidate,
                          "baseline_seconds": row["baseline_median_seconds"],
                          "optimized_seconds": row["median_seconds"], "speedup": row["speedup"],
                          "block_wins": row["block_wins"], "blocks": blocks,
                          "results_equal": row["results_equal"]})
        write_csv(root / "aggregate_results.csv", summaries)
        write_csv(root / "final_query_comparison.csv", final)
        write_csv(root / "all_timings.csv", timings)
        write_csv(root / "all_validations.csv", validations)
        write_json(root / "plan_evidence.json", evidence)
        (root / "benchmark_report.md").write_text(build_report(summaries, final, evidence, metadata, jobs, root), encoding="utf-8")
        metadata["status"] = "completed"
        metadata["all_results_equal"] = all(r["results_equal"] == "True" for r in validations)
        metadata["validation_checks"] = len(validations)
        metadata["benchmark_artifact_bytes_before_export"] = sum(p.stat().st_size for p in root.rglob("*") if p.is_file())
        write_json(root / "suite_manifest.json", metadata)
        exported = export_evidence(root, export_root, jobs) if export_root else None
        print(f"SUITE_COMPLETED={root}\nEXPORTED={exported}", flush=True)
        return root, exported
    except Exception as error:
        metadata.update({"status": "failed", "error": str(error)})
        write_json(root / "suite_manifest.json", metadata)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/config.yaml"))
    parser.add_argument("--blocks", type=int, help="Override measurement/confirmation blocks (default 3)")
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--export-root", type=Path, default=Path("docs/benchmarks/week2"))
    args = parser.parse_args()
    config = load_config(args.config)
    settings = config["data_analysis"]["optimization_benchmark"]
    root = args.output_root or Path(table_path(config, settings["results_root"])) / "suites"
    spark = create_spark()
    try:
        run_suite(spark, config, root, args.export_root, args.blocks)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
