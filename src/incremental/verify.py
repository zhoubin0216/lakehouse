"""Verify the generated Week 3 fixture immediately after applying its release."""
import argparse
import json
from pathlib import Path

from pyspark.sql import functions as F

from src.common import create_spark, load_config, table_path
from src.data_analysis.data_products import PRODUCT_BUILDERS
from src.data_analysis.analytical_queries import register_analysis_views
from src.data_analysis.query_library import ANALYTICAL_QUERIES
from src.data_analysis.result_validation import ResultSnapshot, compare_results
from src.incremental.pipeline import apply_release
from src.incremental.storage import month_predicate, read, save_json, version


def compare_frames(expected, actual):
    actual = actual.select(*expected.columns)
    # Delta relaxes nested array nullability on disk. Check actual value types
    # first, then normalize only nullability for the shared result comparator.
    if expected.schema.simpleString() != actual.schema.simpleString():
        raise AssertionError((expected.schema.simpleString(), actual.schema.simpleString()))
    result = compare_results(ResultSnapshot.build(expected.schema, expected.collect()),
                             ResultSnapshot.build(expected.schema, actual.collect()))
    if not result["results_equal"]:
        raise AssertionError(result)
    return result


def verify(spark, config, manifest_path):
    manifest = json.loads(Path(manifest_path).read_text())
    directory = Path(config["incremental"]["state_root"]) / manifest["release_id"]
    state = json.loads((directory / "state.json").read_text())
    if state["status"] != "complete":
        raise ValueError("Finish or resume the release before verifying")
    report = {"release_id": state["release_id"], "datasets": {}, "products": {}}
    paths = []
    for entry in manifest["datasets"]:
        name = entry["dataset"]
        ds = config["datasets"][name]
        raw_path = table_path(config, ds["raw_table"])
        normal_path = table_path(config, ds["normal_table"])
        paths.extend([raw_path, normal_path])
        raw_before = read(spark, raw_path, state["base"][name]["raw"]).count()
        raw_after = read(spark, raw_path).count()
        metrics = state["datasets"][name]
        assert raw_after == raw_before + metrics["inserted_raw"]
        assert metrics["inserted_raw"] == entry["new_records"]
        assert metrics["ignored_duplicates"] == entry["duplicates"]
        assert metrics["consumption_rejected"] == 0 and metrics["cleaning_rejected"] == 0
        report["datasets"][name] = dict(raw_before=raw_before, raw_after=raw_after,
            normal_rows=read(spark, normal_path).count(), inserted_raw=metrics["inserted_raw"],
            ignored_duplicates=metrics["ignored_duplicates"],
            rejected=metrics["consumption_rejected"] + metrics["cleaning_rejected"])
    integrated_path = table_path(config, config["data_integration"]["integrated_taxi_trips_table"])
    integrated = read(spark, integrated_path)
    paths.append(integrated_path)
    report["integrated_rows"] = integrated.count()
    assert integrated.select("trip_id").distinct().count() == report["integrated_rows"]
    report["new_columns"] = {
        "humidity_nonnull": integrated.filter("pickup_year = 2025 AND humidity IS NOT NULL").count(),
        "aqi_nonnull": integrated.filter("pickup_year = 2025 AND aqi IS NOT NULL").count(),
    }
    for count in report["new_columns"].values():
        assert count == state["datasets"]["yellow_taxi_trips"]["normal_changed"]
    predicate = month_predicate(state.get("affected_months", []))
    scoped = integrated.filter(predicate).cache()
    try:
        for name, definition in config["data_analysis"]["products"]["definitions"].items():
            path = table_path(config, definition["table"])
            paths.append(path)
            actual = read(spark, path)
            check = compare_frames(PRODUCT_BUILDERS[name](scoped), actual.filter(predicate))
            if name in state["base_products"]:
                before = read(spark, path, state["base_products"][name]).filter(f"NOT ({predicate})")
                compare_frames(before, actual.filter(f"NOT ({predicate})"))
            report["products"][name] = dict(total_rows=actual.count(), affected_rows=check["actual_rows"],
                                             matches_recomputation=True, historical_rows_unchanged=True)
    finally:
        scoped.unpersist()
    paths.extend(table_path(config, path) for path in config["data_aggregation"].values())
    paths.append(table_path(config, config["data_analysis"]["products"]["catalog_table"]))
    versions_before = {path: version(spark, path) for path in paths}
    assert apply_release(spark, config, manifest_path)["already_complete"]
    assert versions_before == {path: version(spark, path) for path in paths}
    report["replay_zero_delta_writes"] = True
    register_analysis_views(spark, config)
    report["compatible_queries"] = {}
    for name, sql in ANALYTICAL_QUERIES.items():
        report["compatible_queries"][name] = spark.sql(sql).count()
    for view in ("analysis_weather", "analysis_air_quality"):
        assert spark.table(view).filter("event_hour >= TIMESTAMP_NTZ '2024-04-01' AND event_hour < TIMESTAMP_NTZ '2025-01-01'").count() == 0
    save_json(directory / "verification.json", report)
    print(json.dumps(report, indent=2), flush=True)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path("data/raw/updates/week3_release2/manifest.json"))
    args = parser.parse_args()
    spark = create_spark()
    try:
        verify(spark, load_config(), args.manifest)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
