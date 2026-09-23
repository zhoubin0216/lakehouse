"""Generate deterministic Week 3 update files from the original release."""
import argparse
from pathlib import Path
import re
import shutil

from pyspark.sql import functions as F, Window

from src.common import create_spark, load_config, resolve_schema_definition, table_path
from src.data_consumption.raw_tables import to_snake_case
from src.data_consumption.registry import discover_source_files
from src.incremental.storage import digest, read, save_json


def original_columns(df, definition):
    mapping = definition.get("columns", {})
    return df.select(*[
        F.col(mapping.get(name, to_snake_case(name))).alias(name)
        for name in definition["expected_columns"]
    ])


def write_single(df, path, fmt):
    temporary = path.with_name(path.name + ".parts")
    writer = df.coalesce(1).write.mode("overwrite")
    if fmt == "csv":
        writer.option("header", True).csv(str(temporary))
    else:
        writer.parquet(str(temporary))
    part = next(temporary.glob(f"part-*.{fmt}"))
    part.replace(path)
    shutil.rmtree(temporary)


def generate(spark, config, release="week3_release2",inject_invalid=False,):
    settings = config["incremental"]
    if not re.fullmatch(r"[A-Za-z0-9_-]+", release):
        raise ValueError("Invalid release ID")
    if settings["days"] != 7:
        raise ValueError("The reproducible winter fixture uses seven consecutive days")
    root = Path(settings["releases_root"]) / release
    manifest_path = root / "manifest.json"
    if manifest_path.exists():
        raise FileExistsError(f"Immutable release already exists: {manifest_path}")
    root.mkdir(parents=True, exist_ok=True)
    datasets = config["datasets"]
    raw = {}
    for name, ds in datasets.items():
        definition = resolve_schema_definition(name, ds, 1)[1]
        originals = discover_source_files(name, ds, definition, config)
        raw[name] = read(spark, table_path(config, ds["raw_table"])).filter(
            (F.col("_schema_version") == 1)
            & F.col("_source_file").isin([value for path in originals for value in (str(path), str(path.resolve()))]))
    hours = int(settings["days"]) * 24
    weather_end = raw["weather_hourly"].selectExpr(
        "max(make_timestamp_ntz(year, month, day, hour, 0, 0)) AS t").first().t
    air_end = raw["air_quality"].selectExpr(
        "max(to_timestamp_ntz(concat(cast(date_local as string), ' ', "
        "regexp_extract(time_local, '([0-9]{1,2}:[0-9]{2}(:[0-9]{2})?)$', 1)))) AS t").first().t
    # Hourly releases start at the next full local hour, not the host timezone.
    from datetime import timedelta
    start = weather_end + timedelta(hours=1)
    air_start = air_end.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    if start != air_start:
        raise ValueError("Weather and air coverage differ; generate separate release periods")
    taxi_end = raw["yellow_taxi_trips"].agg(F.max("pickup_timestamp")).first()[0]
    if start <= taxi_end:
        raise ValueError("Update period must follow the latest original taxi pickup")
    clock = spark.range(hours).select(
        "id", (F.lit(start).cast("timestamp_ntz") + F.expr("id * INTERVAL 1 HOUR")).alias("ts"))
    originals = raw["yellow_taxi_trips"]
    original_count = originals.count()
    new_count = int(original_count * settings["new_trip_fraction"])
    duplicate_count = int(original_count * settings["duplicate_trip_fraction"])
    # Sample distinct valid trips; preserve correlated fares/distances/locations.
    pool = originals.filter(
        "pickup_timestamp >= TIMESTAMP_NTZ '2024-01-01' AND "
        "pickup_timestamp < TIMESTAMP_NTZ '2024-04-01' AND "
        "dropoff_timestamp > pickup_timestamp AND "
        "timestampdiff(SECOND,pickup_timestamp,dropoff_timestamp) BETWEEN 1 AND 86400 "
        "AND pickup_location_id IS NOT NULL AND dropoff_location_id IS NOT NULL"
    ).filter(F.pmod(F.xxhash64("_record_hash", F.lit(settings["seed"])), F.lit(1000000))
             < int(settings["new_trip_fraction"] * 2000000)).dropDuplicates(["_record_hash"])
    ordered = pool.orderBy(F.xxhash64("_record_hash", F.lit(settings["seed"])), "_record_hash")
    sample = ordered.limit(new_count).withColumn(
        "n", F.row_number().over(Window.orderBy(F.xxhash64("_record_hash"), "_record_hash")) - 1)
    sample = sample.withColumn("duration", F.expr("timestampdiff(SECOND,pickup_timestamp,dropoff_timestamp)"))
    sample = sample.withColumn("pickup_timestamp", F.lit(start).cast("timestamp_ntz")
                               + F.expr(f"pmod(xxhash64(_record_hash), {hours * 3600}) * INTERVAL 1 SECOND")
                               + F.expr("n * INTERVAL 1 MICROSECOND"))
    sample = sample.withColumn("dropoff_timestamp", F.col("pickup_timestamp") + F.expr("duration * INTERVAL 1 SECOND"))
    taxi_def = resolve_schema_definition("yellow_taxi_trips", datasets["yellow_taxi_trips"], 1)[1]
    taxi = original_columns(sample, taxi_def).unionByName(original_columns(ordered.limit(duplicate_count), taxi_def))

    weather_def = resolve_schema_definition("weather_hourly", datasets["weather_hourly"], 1)[1]
    winter = raw["weather_hourly"].filter("month = 1 AND day <= 7").withColumn(
        "id", ((F.col("day") - 1) * 24 + F.col("hour")).cast("long"))
    weather = clock.join(winter.drop("year", "month", "day", "hour"), "id").withColumn("year", F.year("ts"))
    weather = weather.withColumn("month", F.month("ts")).withColumn("day", F.dayofmonth("ts")).withColumn("hour", F.hour("ts"))
    weather = original_columns(weather, weather_def).withColumn("humidity", F.coalesce(F.col("rhum"), F.lit(65.0)))
    weather = weather.withColumn("humidity", F.greatest(F.lit(20.0), F.least(F.lit(100.0), F.col("humidity"))))

    if inject_invalid:
        # Inject exactly one invalid weather record.
        weather = weather.withColumn(
            "humidity",
            F.when(
                (F.col("year") == start.year)
                & (F.col("month") == start.month)
                & (F.col("day") == start.day)
                & (F.col("hour") == start.hour),
                F.lit(150.0),
            ).otherwise(F.col("humidity")),
        )

    air_def = resolve_schema_definition("air_quality", datasets["air_quality"], 1)[1]
    station_keys = ["state_code", "county_code", "site_num", "parameter_code", "poc"]
    stations = raw["air_quality"].filter(
        "state_name = 'New York' AND county_name IN ('Bronx','Kings','Queens') AND sample_measurement >= 0"
    ).withColumn("rank", F.row_number().over(Window.partitionBy(*station_keys).orderBy("date_local", "time_local", "_record_hash"))).filter("rank = 1")
    air = stations.crossJoin(clock).withColumn("date_local", F.to_date("ts")).withColumn("time_local", F.date_format("ts", "HH:mm"))
    air = air.withColumn("utc", F.col("ts") + F.expr("INTERVAL 5 HOURS"))
    air = air.withColumn("date_gmt", F.to_date("utc")).withColumn("time_gmt", F.date_format("utc", "HH:mm"))
    air = air.withColumn("date_of_last_change", F.to_date("ts"))
    air = air.withColumn("sample_measurement", (F.lit(4.0) + F.pmod(F.xxhash64(*station_keys, "id"), F.lit(220)) / 10).cast("double"))
    air = original_columns(air, air_def).withColumn("aqi", F.round(F.col("Sample Measurement") * 3, 0))

    if inject_invalid:
        # Deterministically inject exactly one invalid AQI record.
        validation_window = Window.orderBy(
            "State Code",
            "County Code",
            "Site Num",
            "POC",
            "Date Local",
            "Time Local",
        )

        air = (
            air
            .withColumn(
                "_validation_row",
                F.row_number().over(validation_window),
            )
            .withColumn(
                "aqi",
                F.when(
                    F.col("_validation_row") == 1,
                    F.lit(-1.0),
                ).otherwise(F.col("aqi")),
            )
            .drop("_validation_row")
        )

    zones_def = resolve_schema_definition("taxi_zone_lookup", datasets["taxi_zone_lookup"], 1)[1]
    zones = original_columns(raw["taxi_zone_lookup"].limit(0), zones_def)
    outputs = {"yellow_taxi_trips": (taxi, "taxi_updates.parquet", 1),
               "weather_hourly": (weather, "weather_updates.csv", 2),
               "air_quality": (air, "air_quality_updates.csv", 2),
               "taxi_zone_lookup": (zones, "taxi_zone_updates.csv", 1)}
    entries = []
    for name, (df, filename, schema_version) in outputs.items():
        path = root / filename
        count = df.count()
        if name == "yellow_taxi_trips" and count != new_count + duplicate_count:
            raise ValueError("Insufficient distinct sample candidates for the configured fractions")
        write_single(df, path, path.suffix[1:])
        if name == "yellow_taxi_trips":
            stored = spark.read.parquet(str(path))
            new_trips = stored.filter(F.col("tpep_pickup_datetime") >= F.lit(start).cast("timestamp_ntz"))
            invalid = new_trips.filter("timestampdiff(SECOND,tpep_pickup_datetime,tpep_dropoff_datetime) NOT BETWEEN 1 AND 86400")
            if invalid.limit(1).count() or new_trips.count() != new_count:
                raise ValueError("Generated taxi release failed duration/count validation")
        entries.append(dict(dataset=name, file=filename, schema_version=schema_version,
                            records=count, new_records=new_count if name == "yellow_taxi_trips" else count,
                            duplicates=duplicate_count if name == "yellow_taxi_trips" else 0,
                            sha256=digest(path), added_columns={"weather_hourly": ["humidity"], "air_quality": ["aqi"]}.get(name, [])))
        print(f"Generated {path}: {count} rows", flush=True)
    manifest = dict(release_id=release, synthetic=True, start=str(start), hours=hours,
                    taxi_original_records=original_count, fraction_denominator="original taxi raw row count",
                    aqi_note="Synthetic illustrative AQI, not an official regulatory calculation",
                    zone_note="Unchanged static lookup: header-only update file", datasets=entries)
    save_json(manifest_path, manifest)
    return manifest_path


def main():
    parser = argparse.ArgumentParser(
        description=__doc__
    )

    parser.add_argument(
        "--release",
        default="week3_release2",
    )

    parser.add_argument(
        "--inject-invalid",
        action="store_true",
        help="Inject a small number of invalid records for validation monitoring tests.",
    )

    args = parser.parse_args()

    spark = create_spark()

    try:
        print(
            generate(
                spark,
                load_config(),
                args.release,
                inject_invalid=args.inject_invalid,
            )
        )
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
