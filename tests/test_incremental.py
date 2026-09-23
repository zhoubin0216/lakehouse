from copy import deepcopy
from datetime import datetime
import json

import pytest
from pyspark.sql import functions as F

from src.common import load_config, table_path, write_delta
from src.data_analysis.data_products import PRODUCT_BUILDERS, build_data_products
from src.incremental.pipeline import novel_records, validate_manifest
from src.incremental.refresh import affected_product_rows
from src.incremental.storage import (
    changed_rows, digest, fit_legacy_raw, merge_rows, read, replace_scope, save_json, version,
)
from test_data_products import integrated_fixture


def raw_weather(spark, values):
    return spark.createDataFrame(values, "year int, month int, day int, hour int, temp double, "
        "_schema_version int, _ingestion_timestamp timestamp, _record_hash string")


def test_novel_records_ignore_duplicates_allow_corrections_and_reversions(spark):
    initial = raw_weather(spark, [(2024, 1, 1, 0, 2.0, 1, datetime(2024, 1, 1), "a")])
    duplicate = initial.unionByName(initial)
    assert novel_records(duplicate, initial, "weather_hourly").count() == 0
    corrected = raw_weather(spark, [(2024, 1, 1, 0, 3.0, 2, datetime(2024, 2, 1), "b")])
    assert novel_records(corrected, initial, "weather_hourly").count() == 1
    # A later v1 record can legitimately revert an earlier value.
    reverted = initial.withColumn("_ingestion_timestamp", F.lit(datetime(2024, 3, 1)))
    assert novel_records(reverted, initial.unionByName(corrected), "weather_hourly").count() == 1
    with pytest.raises(ValueError, match="conflicting revisions"):
        novel_records(initial.unionByName(corrected), initial, "weather_hourly")


def test_null_safe_record_identity_and_schema_additions(spark):
    before = raw_weather(spark, [(2024, 1, 1, 0, None, 1, datetime(2024, 1, 1), "a")])
    after = before.withColumn("humidity", F.lit(60.0))
    assert novel_records(after, before, "weather_hourly").count() == 1
    assert changed_rows(after.select("year", "temp", "humidity"),
                        before.select("year", "temp"), ["year"]).count() == 1


def test_air_keys_accept_both_duration_and_clock_format(spark):
    from src.data_cleaning.normal_tables import normalize_air_times
    original = spark.createDataFrame([("2024-03-31", "0 days 02:00:00")], "date_local string, time_local string")
    update = spark.createDataFrame([("2024-03-31", "02:00")], original.schema)
    assert normalize_air_times(original).first().time_local == "02:00:00"
    assert normalize_air_times(update).first().time_local == "02:00:00"


def test_legacy_raw_bridge_is_lossless_and_preserves_source_payload(spark):
    old = spark.createDataFrame([(60, "")], "rhum int, snwd string")
    incoming = spark.createDataFrame([(60.0, 1.5)], "rhum double, snwd double")
    adapted = fit_legacy_raw(incoming, old).first()
    assert adapted.rhum == 60 and adapted.snwd == "1.5"
    assert json.loads(adapted._source_payload) == {"rhum": 60.0, "snwd": 1.5}
    with pytest.raises(ValueError, match="without loss"):
        fit_legacy_raw(incoming.withColumn("rhum", F.lit(60.5)), old)


def test_legacy_time_bridge_keeps_dst_hour(spark):
    from src.data_cleaning.normal_tables import normalize_air_times
    old = spark.createDataFrame([(datetime(2024, 1, 1, 2),)], "time_local timestamp")
    incoming = spark.createDataFrame([("02:00", "2024-03-31")], "time_local string, date_local string")
    adapted = fit_legacy_raw(incoming, old)
    assert normalize_air_times(adapted).first().time_local == "02:00:00"


def test_merge_evolves_nullable_columns_preserves_history_and_is_idempotent(spark, tmp_path):
    path = tmp_path / "table"
    merge_rows(spark.createDataFrame([(1, "old")], "id int, value string"), path, ["id"])
    rows = spark.createDataFrame([(2, "new", 60.0)], "id int, value string, humidity double")
    merge_rows(rows, path, ["id"])
    merge_rows(rows, path, ["id"])
    result = read(spark, path).orderBy("id").collect()
    assert len(result) == 2
    assert result[0].humidity is None
    assert result[1].humidity == 60.0
    assert read(spark, path, 0).count() == 1


def test_scoped_replace_removes_obsolete_groups_but_preserves_other_months(spark, tmp_path):
    path = tmp_path / "product"
    before = spark.createDataFrame([(1, "old", 3), (2, "keep", 9)], "month int, category string, count long")
    replace_scope(before, path, "true")
    replacement = spark.createDataFrame([(1, "new", 3)], before.schema)
    replace_scope(replacement, path, "month = 1")
    assert {(r.month, r.category) for r in read(spark, path).collect()} == {(1, "new"), (2, "keep")}
    replace_scope(replacement.limit(0), path, "month = 1")
    assert read(spark, path).first().category == "keep"


def test_only_dependent_products_are_affected(spark):
    before = integrated_fixture(spark)
    after = before.withColumn("humidity", F.lit(75.0))
    for name in PRODUCT_BUILDERS:
        assert affected_product_rows(before, after, name).count() == 0
    changed = before.withColumn("weather_condition_code", F.lit(7))
    assert affected_product_rows(before, changed, "weather_impact_summary").count() > 0
    assert affected_product_rows(before, changed, "daily_mobility_summary").count() == 0


def test_partial_products_match_full_recomputation_and_keep_old_metadata(spark, tmp_path):
    config = deepcopy(load_config())
    config["paths"]["lakehouse"] = str(tmp_path)
    source = table_path(config, config["data_integration"]["integrated_taxi_trips_table"])
    initial = integrated_fixture(spark)
    write_delta(initial, source)
    build_data_products(spark, config)
    definitions = config["data_analysis"]["products"]["definitions"]
    original_dates = {}
    for name, definition in definitions.items():
        original_dates[name] = read(spark, table_path(config, definition["table"])).filter("pickup_month = 2").first()._refreshed_at
    updated = initial.withColumn("weather_condition_code", F.when(F.col("pickup_month") == 1, 7).otherwise(F.col("weather_condition_code")))
    updated = updated.withColumn("fare_amount", F.when(F.col("trip_id") == "a", 13.0).otherwise(F.col("fare_amount")))
    write_delta(updated, source)
    for name, builder in PRODUCT_BUILDERS.items():
        build_data_products(spark, config, name, scope_predicate="pickup_year = 2024 AND pickup_month = 1")
        actual = read(spark, table_path(config, definitions[name]["table"]))
        expected = builder(updated)
        assert actual.select(*expected.columns).exceptAll(expected).count() == 0
        assert expected.exceptAll(actual.select(*expected.columns)).count() == 0
        assert actual.filter("pickup_month = 2").first()._refreshed_at == original_dates[name]


def test_manifest_detects_mutated_release_files(tmp_path):
    file = tmp_path / "zones.csv"
    file.write_text("LocationID,Borough,Zone,service_zone\n")
    manifest = {"release_id": "test", "datasets": [dict(dataset="taxi_zone_lookup", file=file.name, schema_version=1, sha256=digest(file))]}
    path = tmp_path / "manifest.json"
    save_json(path, manifest)
    validate_manifest(path, load_config())
    file.write_text("changed")
    with pytest.raises(ValueError, match="has changed"):
        validate_manifest(path, load_config())


def test_completed_release_can_be_skipped_while_another_is_pending(tmp_path):
    from src.incremental.pipeline import _apply_release
    config = deepcopy(load_config())
    path = tmp_path / "manifest.json"
    save_json(path, {"release_id": "old", "datasets": []})
    save_json(tmp_path / "old" / "state.json", dict(status="complete", manifest_sha256=digest(path)))
    save_json(tmp_path / "new" / "state.json", dict(status="failed", manifest="pending.json"))
    assert _apply_release(None, config, path, json.loads(path.read_text()), tmp_path)["already_complete"]
    with pytest.raises(RuntimeError, match="Resume pending"):
        _apply_release(None, config, path, {"release_id": "third"}, tmp_path)


def test_release_resumes_after_raw_and_normal_commit(spark, tmp_path, monkeypatch):
    import csv
    import src.incremental.pipeline as pipeline
    from src.data_consumption.raw_tables import add_lineage_columns
    from src.data_cleaning.normal_tables import clean_weather
    config = deepcopy(load_config())
    config["paths"]["lakehouse"] = str(tmp_path / "lakehouse")
    config["incremental"]["state_root"] = str(tmp_path / "state")
    ds = config["datasets"]["weather_hourly"]
    definition = ds["schema_versions"]["1"]
    values = {column: ("fixture" if dtype == "string" else 1 if dtype == "int" else 50.0)
              for column, dtype in definition["column_types"].items()}
    values.update(year=2024, month=1, day=1, hour=0, temp=5.0)
    schema = ", ".join(f"`{column}` {dtype}" for column, dtype in definition["column_types"].items())
    raw = spark.createDataFrame([tuple(values[column] for column in definition["column_types"])], schema)
    raw = add_lineage_columns(raw, "weather_hourly", 1, "initial", dict(path="fixture", size_bytes=1, modified_time_ns=1700000000000000000))
    for name, dataset in config["datasets"].items():
        base = raw if name == "weather_hourly" else spark.range(1)
        write_delta(base, table_path(config, dataset["raw_table"]))
        write_delta(clean_weather(raw) if name == "weather_hourly" else base,
                    table_path(config, dataset["normal_table"]))
    write_delta(integrated_fixture(spark), table_path(config, config["data_integration"]["integrated_taxi_trips_table"]))
    source = tmp_path / "weather.csv"
    values.update(temp=7.0, humidity=65.0)
    with source.open("w") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(values))
        writer.writeheader()
        writer.writerow(values)
    manifest = tmp_path / "manifest.json"
    save_json(manifest, dict(release_id="resume", datasets=[dict(dataset="weather_hourly", file=source.name,
        sha256=digest(source), schema_version=2)]))
    def fail(*args):
        raise RuntimeError("simulated downstream interruption")
    monkeypatch.setattr(pipeline, "integrate_changes", fail)
    with pytest.raises(RuntimeError, match="interruption"):
        pipeline.apply_release(spark, config, manifest)
    assert read(spark, table_path(config, ds["raw_table"])).count() == 2
    assert read(spark, table_path(config, ds["normal_table"])).first().temperature_c == 7.0
    def resume(spark, config, directory, state, checkpoint):
        state["integrated_changed"] = 0
        return integrated_fixture(spark).limit(0)
    monkeypatch.setattr(pipeline, "integrate_changes", resume)
    pipeline.apply_release(spark, config, manifest)
    versions = [version(spark, table_path(config, ds[f"{layer}_table"])) for layer in ("raw", "normal")]
    assert pipeline.apply_release(spark, config, manifest)["already_complete"]
    assert versions == [version(spark, table_path(config, ds[f"{layer}_table"])) for layer in ("raw", "normal")]
    assert read(spark, table_path(config, ds["raw_table"])).count() == 2


def test_late_weather_update_reenriches_only_matching_trips(spark, tmp_path):
    from src.data_integration.integrated_tables import integrate_taxi_trips
    from src.incremental.pipeline import integrate_changes
    config = deepcopy(load_config())
    config["paths"]["lakehouse"] = str(tmp_path / "lakehouse")
    ds = config["datasets"]
    fixture = integrated_fixture(spark)
    taxi = fixture.select("trip_id", "pickup_timestamp", "pickup_date", "pickup_year", "pickup_month",
                         "pickup_location_id", "fare_amount", "total_amount", "trip_distance",
                         "passenger_count", "trip_duration_seconds",
                         F.col("taxi_schema_version").alias("source_schema_version"))
    taxi = taxi.withColumn("dropoff_location_id", F.col("pickup_location_id")).withColumn(
        "pickup_hour", F.date_trunc("hour", "pickup_timestamp").cast("timestamp_ntz"))
    zones = spark.createDataFrame([(1, "A", "Manhattan", "Yellow", 1), (2, "B", "Queens", "Yellow", 1)],
                                  "location_id int, zone string, borough string, service_zone string, source_schema_version int")
    weather = spark.createDataFrame([(datetime(2024, 1, 1, 10), 5.0, 1, 1)],
        "event_hour timestamp_ntz, temperature_c double, weather_condition_code int, source_schema_version int")
    for field in ("relative_humidity_pct", "precipitation_mm", "snow_depth_mm", "wind_direction_deg",
                  "wind_speed_kmh", "wind_gust_kmh", "pressure_hpa", "cloud_cover_pct"):
        weather = weather.withColumn(field, F.lit(1.0))
    air = spark.createDataFrame([], "event_hour timestamp_ntz, pm25_avg_ug_m3 double, pm25_min_ug_m3 double, "
        "pm25_max_ug_m3 double, air_quality_observation_count long, air_quality_site_count long, source_schema_versions array<int>")
    for name, df in [("yellow_taxi_trips", taxi), ("taxi_zone_lookup", zones),
                     ("weather_hourly", weather), ("air_quality", air)]:
        write_delta(df, table_path(config, ds[name]["normal_table"]))
    target = table_path(config, config["data_integration"]["integrated_taxi_trips_table"])
    initial = integrate_taxi_trips(taxi, zones, weather, air)
    write_delta(initial, target, partitions=["pickup_year", "pickup_month"])
    changed = weather.withColumn("temperature_c", F.lit(7.0)).withColumn("weather_condition_code", F.lit(7))
    write_delta(changed, table_path(config, ds["weather_hourly"]["normal_table"]))
    directory = tmp_path / "state"
    write_delta(changed, str(directory / "normal_weather_hourly"))
    state = {"datasets": {"weather_hourly": {"normal_changed": 1}}, "base_integrated": 0}
    integrate_changes(spark, config, directory, state, lambda: None)
    assert state["integrated_changed"] == 2
    result = {r.trip_id: r for r in read(spark, target).collect()}
    assert result["a"].temperature_c == result["b"].temperature_c == 7.0
    assert result["c"].temperature_c is None and result["d"].temperature_c is None
