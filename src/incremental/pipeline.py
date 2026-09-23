"""Apply a release without overwriting historical raw or unaffected partitions."""
import hashlib
import json
from pathlib import Path
import re
import time

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.common import resolve_schema_definition, table_path
from src.data_consumption.raw_tables import load_source_files
from src.data_cleaning.normal_tables import (
    clean_taxi_trips_with_rejections, clean_taxi_zones_with_rejections,
    clean_weather_with_rejections, clean_air_quality_with_rejections,
    latest_schema_records,
    normalize_air_times,
)
from src.data_integration.integrated_tables import integrate_taxi_trips
from src.data_quality import rejected_table_path
from src.data_analysis.data_products import build_data_products
from src.incremental.storage import (
    align, changed_rows, digest, fit_legacy_raw, merge_rows, month_predicate, read,
    save_json, version, writer_lock,
)
from src.incremental.refresh import affected_product_rows, refresh_aggregates


RAW_KEYS = {
    "yellow_taxi_trips": ["_record_hash"],
    "taxi_zone_lookup": ["location_id"],
    "weather_hourly": ["year", "month", "day", "hour"],
    "air_quality": ["state_code", "county_code", "site_num", "parameter_code", "poc", "date_local", "time_local"],
}
NORMAL_KEYS = {"yellow_taxi_trips": ["trip_id"], "taxi_zone_lookup": ["location_id"],
               "weather_hourly": ["event_hour"], "air_quality": ["event_hour"]}
CLEANERS = {"taxi_zone_lookup": clean_taxi_zones_with_rejections,
            "weather_hourly": clean_weather_with_rejections,
            "air_quality": clean_air_quality_with_rejections}


def novel_records(incoming, previous, dataset):
    """Ignore exact retries, but retain a new revision of a stable business key."""
    keys = RAW_KEYS[dataset]
    if dataset == "air_quality":
        incoming, previous = normalize_air_times(incoming), normalize_air_times(previous)
    fields = sorted(c for c in incoming.columns if not c.startswith("_"))
    incoming = incoming.withColumn("_content_hash", F.sha2(F.to_json(
        F.struct(*fields), options={"ignoreNullFields": "false"}), 256)).dropDuplicates(["_content_hash"])
    if dataset != "yellow_taxi_trips":
        if incoming.groupBy(*keys).count().filter("count > 1").limit(1).count():
            raise ValueError(f"{dataset}: conflicting revisions for a key in the same release")
    previous = previous.join(incoming.select(*keys).distinct(), keys, "left_semi")
    if dataset != "yellow_taxi_trips":
        previous = latest_schema_records(previous, keys)
    previous = align(previous, incoming.schema).withColumn("_content_hash", F.sha2(F.to_json(
        F.struct(*[F.col(c).cast(incoming.schema[c].dataType).alias(c) for c in fields]),
        options={"ignoreNullFields": "false"}), 256))
    return incoming.join(previous.select("_content_hash").distinct(), "_content_hash", "left_anti")


def validate_manifest(path, config):
    path = Path(path).resolve()
    manifest = json.loads(path.read_text())
    if not re.fullmatch(r"[A-Za-z0-9_-]+", manifest["release_id"]):
        raise ValueError("release_id must contain only letters, digits, underscores or hyphens")
    seen = set()
    for entry in manifest["datasets"]:
        name = entry["dataset"]
        if name in seen:
            raise ValueError("One file per dataset is required in a release")
        seen.add(name)
        resolve_schema_definition(name, config["datasets"][name], entry["schema_version"])
        source = (path.parent / entry["file"]).resolve()
        if not source.is_relative_to(path.parent) or not source.is_file():
            raise ValueError(f"Invalid release file: {source}")
        if digest(source) != entry["sha256"]:
            raise ValueError(f"Immutable release file has changed: {source}")
    return manifest


def save_rejections(df, config, dataset, stage, release):
    count = df.count()
    if count:
        (df.withColumn("_release_id", F.lit(release)).write.format("delta")
         .mode("append").option("mergeSchema", "true")
         .option("txnAppId", f"{release}:{dataset}:{stage}:rejects").option("txnVersion", 0)
         .save(rejected_table_path(config, stage, dataset)))
    return count


def process_source(spark, config, manifest_path, entry, directory, state, checkpoint):
    name = entry["dataset"]
    ds = config["datasets"][name]
    release = state["release_id"]
    progress = state["datasets"].setdefault(name, {})
    staged = directory / f"raw_{name}"
    if "staged" not in progress:
        _, definition = resolve_schema_definition(name, ds, entry["schema_version"])
        previous = read(spark, table_path(config, ds["raw_table"]), state["base"][name]["raw"])
        accepted, rejected = load_source_files(spark, [manifest_path.parent / entry["file"]],
            name, entry["schema_version"], definition, config, release)
        # Original lineage used literal-inferred integers; a small CSV update
        # must still fit the original large-file metadata column's long type.
        for field in previous.schema:
            if field.name in ("_source_file_size", "_source_modified_time"):
                accepted = accepted.withColumn(field.name, F.col(field.name).cast(field.dataType))
        rejected_count = save_rejections(rejected, config, name, "consumption", release)
        accepted = fit_legacy_raw(accepted, previous)
        # Materialize strict CSV casts before key aggregation. Otherwise Spark
        # 3.5 expands their inferred constraints combinatorially in wide plans.
        parsed = directory / f"parsed_{name}"
        accepted.write.format("delta").mode("overwrite").save(str(parsed))
        accepted = read(spark, parsed)
        input_count = accepted.count()
        if "records" in entry and input_count + rejected_count != entry["records"]:
            raise ValueError(f"{name}: parsed count does not match the release manifest")
        novel_records(accepted, previous, name).write.format("delta").mode("overwrite").save(str(staged))
        count = read(spark, staged).count()
        progress.update(staged=True, input_records=input_count + rejected_count,
                        inserted_raw=count, ignored_duplicates=input_count-count,
                        consumption_rejected=rejected_count, schema_version=entry["schema_version"])
        checkpoint()
    incoming = read(spark, staged)
    if not progress.get("raw_done"):
        if progress["inserted_raw"]:
            storage_rows = fit_legacy_raw(incoming, read(spark, table_path(config, ds["raw_table"])))
            (storage_rows.write.format("delta").mode("append").option("mergeSchema", "true")
             .option("txnAppId", f"{release}:{name}:raw").option("txnVersion", 0)
             .save(table_path(config, ds["raw_table"])))
        progress["raw_done"] = True
        checkpoint()
    normal_stage = directory / f"normal_{name}"
    if not progress["inserted_raw"]:
        progress.update(normal_changed=0, cleaning_rejected=0, normal_staged=True, normal_done=True)
        checkpoint()
        print(f"{name}: no new content; normal unchanged", flush=True)
        return
    if not progress.get("normal_staged"):
        # Recompute full affected hours for AQ, not only the new stations.
        if name == "yellow_taxi_trips":
            affected_raw = incoming
        else:
            raw = read(spark, table_path(config, ds["raw_table"]))
            if name == "air_quality":
                raw = normalize_air_times(raw)
            keys = ["date_local", "time_local"] if name == "air_quality" else RAW_KEYS[name]
            affected_raw = raw.join(incoming.select(*keys).distinct(), keys, "left_semi")
        cleaner = CLEANERS.get(name)
        normal, rejects = cleaner(affected_raw) if cleaner else clean_taxi_trips_with_rejections(affected_raw, ds)
        previous = read(spark, table_path(config, ds["normal_table"]), state["base"][name]["normal"])
        changed_rows(normal, previous, NORMAL_KEYS[name]).write.format("delta").mode("overwrite").save(str(normal_stage))
        progress["normal_changed"] = read(spark, normal_stage).count()
        progress["cleaning_rejected"] = save_rejections(rejects, config, name, "cleaning", release)
        progress["normal_staged"] = True
        checkpoint()
    if not progress.get("normal_done"):
        if progress["normal_changed"]:
            merge_rows(read(spark, normal_stage), table_path(config, ds["normal_table"]),
                       NORMAL_KEYS[name], ds.get("partitions"))
        progress["normal_done"] = True
        checkpoint()
    print(f"{name}: {progress}", flush=True)


def integrate_changes(spark, config, directory, state, checkpoint):
    target = table_path(config, config["data_integration"]["integrated_taxi_trips_table"])
    staged = directory / "integrated"
    old_stage = directory / "integrated_before"
    if not state.get("integrated_staged"):
        normal = {name: read(spark, table_path(config, ds["normal_table"]))
                  for name, ds in config["datasets"].items()}
        taxi = normal["yellow_taxi_trips"]
        affected = taxi.limit(0)
        for name, progress in state["datasets"].items():
            if not progress["normal_changed"]:
                continue
            change = read(spark, directory / f"normal_{name}")
            if name == "yellow_taxi_trips":
                selected = change
            elif name == "taxi_zone_lookup":
                ids = [r.location_id for r in change.select("location_id").collect()]
                selected = taxi.filter(F.col("pickup_location_id").isin(ids) | F.col("dropoff_location_id").isin(ids))
            else:
                months = [tuple(r) for r in change.select(F.year("event_hour"), F.month("event_hour")).distinct().collect()]
                selected = taxi.filter(month_predicate(months)).join(
                    change.select(F.col("event_hour").alias("pickup_hour")), "pickup_hour", "left_semi")
            affected = affected.unionByName(selected, allowMissingColumns=True)
        taxi = affected.dropDuplicates(["trip_id"])
        candidate = integrate_taxi_trips(taxi, normal["taxi_zone_lookup"], normal["weather_hourly"], normal["air_quality"])
        months = [tuple(r) for r in taxi.select("pickup_year", "pickup_month").distinct().collect()]
        previous = read(spark, target, state["base_integrated"]).filter(month_predicate(months))
        changed_rows(candidate, previous, ["trip_id"]).write.format("delta").mode("overwrite").save(str(staged))
        changes = read(spark, staged)
        previous.join(changes.select("trip_id"), "trip_id", "left_semi").write.format("delta").mode("overwrite").save(str(old_stage))
        state["integrated_changed"] = changes.count()
        state["integrated_staged"] = True
        checkpoint()
    if not state.get("integrated_done"):
        if state["integrated_changed"]:
            merge_rows(read(spark, staged), target, ["trip_id"], config["data_integration"].get("partitions"))
        state["integrated_done"] = True
        state["integrated_version"] = version(spark, target)
        checkpoint()
    return read(spark, staged).unionByName(read(spark, old_stage), allowMissingColumns=True)


def apply_release(spark, config, manifest_path):
    manifest_path = Path(manifest_path).resolve()
    manifest = validate_manifest(manifest_path, config)
    root = Path(config["incremental"]["state_root"])
    setting = "spark.databricks.delta.snapshotPartitions"
    previous = spark.conf.get(setting, "50")
    try:
        spark.conf.set(setting, str(spark.sparkContext.defaultParallelism))
        with writer_lock(root):
            return _apply_release(spark, config, manifest_path, manifest, root)
    finally:
        spark.conf.set(setting, previous)


def _apply_release(spark, config, manifest_path, manifest, root):
    started = time.perf_counter()
    release = manifest["release_id"]
    directory = root / release
    state_path = directory / "state.json"
    fingerprint = hashlib.sha256(json.dumps(dict(config=config, manifest=manifest), sort_keys=True).encode()).hexdigest()
    state = None
    if state_path.exists():
        state = json.loads(state_path.read_text())
        if state["manifest_sha256"] != digest(manifest_path):
            raise ValueError("A processed manifest must be immutable")
        if state["status"] == "complete":
            print(f"{release}: already complete; no Delta writes", flush=True)
            return dict(release_id=release, already_complete=True, inserted_raw=0)
        if state["fingerprint"] != fingerprint:
            raise ValueError("Release/config changed after execution started; use a new release ID or explicit migration")
    for other in root.glob("*/state.json"):
        pending = json.loads(other.read_text())
        if other != state_path and pending["status"] != "complete":
            raise RuntimeError(f"Resume pending release first: {pending['manifest']}")
    if state is None:
        state = dict(release_id=release, manifest=str(manifest_path), fingerprint=fingerprint,
                     manifest_sha256=digest(manifest_path),
                     status="running", datasets={}, products_done=[], base={})
        for name, ds in config["datasets"].items():
            state["base"][name] = {layer: version(spark, table_path(config, ds[f"{layer}_table"]))
                                    for layer in ["raw", "normal"]}
        state["base_integrated"] = version(spark, table_path(config, config["data_integration"]["integrated_taxi_trips_table"]))
        state["base_products"] = {}
        for name, definition in config["data_analysis"]["products"]["definitions"].items():
            path = table_path(config, definition["table"])
            if DeltaTable.isDeltaTable(spark, path):
                state["base_products"][name] = version(spark, path)
        save_json(state_path, state)
    def checkpoint():
        save_json(state_path, state)
    state["status"] = "running"
    state.pop("error", None)
    checkpoint()
    try:
        for entry in manifest["datasets"]:
            process_source(spark, config, manifest_path, entry, directory, state, checkpoint)
        scope = integrate_changes(spark, config, directory, state, checkpoint)
        if state["integrated_changed"]:
            months = [tuple(r) for r in scope.select("pickup_year", "pickup_month").distinct().collect()]
            dates = [str(r.pickup_date) for r in scope.select("pickup_date").distinct().collect()]
            state["affected_months"], state["affected_dates"] = months, dates
            if not state.get("aggregate_done"):
                refresh_aggregates(spark, config, scope)
                state["aggregate_done"] = True
                checkpoint()
            for name in config["data_analysis"]["products"]["definitions"]:
                if name not in state["products_done"]:
                    from src.incremental.storage import values_predicate
                    impacted = affected_product_rows(read(spark, directory / "integrated_before"),
                                                     read(spark, directory / "integrated"), name)
                    product_months = [tuple(r) for r in impacted.select("pickup_year", "pickup_month").distinct().collect()]
                    if product_months:
                        product_dates = [r[0] for r in impacted.select("pickup_date").distinct().collect()]
                        predicate = month_predicate(product_months)
                        if name == "daily_mobility_summary":
                            predicate = f"({predicate}) AND {values_predicate('pickup_date', product_dates)}"
                        build_data_products(spark, config, name, scope_predicate=predicate,
                                            source_version=state["integrated_version"])
                    else:
                        state.setdefault("products_skipped", []).append(name)
                    state["products_done"].append(name)
                    checkpoint()
        state["status"] = "complete"
        state["last_attempt_seconds"] = time.perf_counter() - started
        state.pop("error", None)
        checkpoint()
        return state
    except Exception as error:
        state["status"] = "failed"
        state["error"] = str(error)
        checkpoint()
        raise
