"""Week 4 Task 4: compare raw-file and integrated-platform ML workflows.

Approach A starts from the original Taxi Trips, Weather, Air Quality, and Taxi
Zone Lookup files and performs the ML-required loading, cleaning, integration,
and feature preparation locally in this module.

Approach B starts from the integrated Delta table produced by Weeks 1-3.

Both approaches are then passed through the same Task 1 demand-dataset function,
the same Task 2 feature pipeline, and the same RandomForestRegressor.  The goal
is to isolate the role of the data-engineering platform rather than compare two
different ML implementations.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import statistics
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, FloatType, DecimalType, NumericType
from pyspark.sql import functions as F

from src.common import (
    create_spark,
    load_config,
    read_delta,
    resolve_schema_definition,
    table_path,
)
from src.machine_learning.feature_pipeline import (
    fit_feature_pipeline,
    transform_feature_dataset,
)
from src.machine_learning.training_dataset import (
    filter_observation_period,
    hourly_taxi_demand_dataset,
)


# The comparison is deliberately limited to the four source datasets named in
# the assignment.  These names match the existing project configuration.
RAW_DATASETS = (
    "yellow_taxi_trips",
    "weather_hourly",
    "air_quality",
    "taxi_zone_lookup",
)

# Human-readable evidence for the "preprocessing complexity" part of Task 4.
APPROACH_A_STAGES = (
    "read four original source datasets",
    "canonicalize source columns",
    "recreate source record hashes and deduplicate taxi records",
    "apply Week 1 taxi validity rules",
    "normalize taxi pickup hour",
    "clean taxi-zone reference data",
    "clean/aggregate hourly weather",
    "clean/aggregate hourly PM2.5",
    "join pickup zone",
    "join weather by pickup hour",
    "join air quality by pickup hour",
    "construct zone-hour demand dataset",
    "fit/apply shared feature pipeline",
)

APPROACH_B_STAGES = (
    "read integrated Delta table",
    "construct zone-hour demand dataset",
    "fit/apply shared feature pipeline",
)


@dataclass
class RunMetrics:
    repeat: int
    order: str
    approach: str
    preparation_seconds: float
    feature_seconds: float
    training_seconds: float
    total_seconds: float
    rows: int
    train_rows: int
    validation_rows: int
    test_rows: int
    demand_sum: int
    zero_demand_rows: int
    distinct_zones: int
    first_hour: str | None
    last_hour: str | None


@dataclass
class TrainingResult:
    seconds: float
    prediction_rows: int


def _snake_case(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "column"


def _source_path(config: dict, dataset_config: dict) -> str:
    """Return the original configured source path, including any Spark glob."""
    return str(Path(config["paths"]["raw"]) / dataset_config["source"])


def _try_cast(column: str, expected_type: str):
    """Mirror Week 1 consumption-stage safe casting without calling the platform."""
    escaped = column.replace("`", "``")
    return F.expr(f"try_cast(`{escaped}` AS {expected_type})")


def _read_original_source(
    spark: SparkSession,
    config: dict,
    dataset_name: str,
) -> DataFrame:
    """Read and type-check an original source file directly.

    This reproduces the source-contract behavior from Week 1 ingestion while
    keeping Approach A independent of the raw/normal/integrated Delta tables.
    """
    dataset_config = config["datasets"][dataset_name]
    source_version = dataset_config.get("source_schema_version")
    _, schema = resolve_schema_definition(dataset_name, dataset_config, source_version)
    path = _source_path(config, dataset_config)

    if schema["format"] == "parquet":
        df = spark.read.parquet(path)
        expected = schema.get("column_types", {})
        actual = {f.name: f.dataType.simpleString() for f in df.schema.fields}
        mismatches = {
            c: {"expected": expected[c], "actual": actual.get(c)}
            for c in schema["expected_columns"] if actual.get(c) != expected[c]
        }
        if mismatches:
            raise ValueError(f"Approach A parquet schema mismatch for {dataset_name}: {mismatches}")
    elif schema["format"] == "csv":
        reader = spark.read
        for key, value in schema.get("read_options", {}).items():
            reader = reader.option(key, value)
        df = reader.csv(path)
    else:
        raise ValueError(f"Approach A does not support source format {schema['format']!r} for {dataset_name}")

    expected_columns = set(schema["expected_columns"])
    actual_columns = set(df.columns)
    missing = sorted(expected_columns - actual_columns)
    unexpected = sorted(actual_columns - expected_columns)
    if missing or unexpected:
        raise ValueError(
            f"Approach A source columns do not match {dataset_name} contract; "
            f"missing={missing}, unexpected={unexpected}"
        )

    if schema["format"] == "csv":
        casted = []
        valid_condition = F.lit(True)
        for column in schema["expected_columns"]:
            expected_type = schema["column_types"][column]
            source = F.col(column)
            cast_value = source.cast("string") if expected_type == "string" else _try_cast(column, expected_type)
            has_value = source.isNotNull() & (F.trim(source.cast("string")) != "")
            valid_condition = valid_condition & ~(has_value & cast_value.isNull())
            casted.append(cast_value.alias(column))
        df = df.filter(valid_condition).select(*casted)

    mapping = schema.get("columns", {})
    used: set[str] = set()
    for source in list(df.columns):
        target = mapping.get(source, _snake_case(source))
        original_target = target
        suffix = 2
        while target in used:
            target = f"{original_target}_{suffix}"
            suffix += 1
        used.add(target)
        if source != target:
            df = df.withColumnRenamed(source, target)
    return df


def _require_columns(df: DataFrame, columns: Iterable[str], label: str) -> None:
    missing = sorted(set(columns) - set(df.columns))
    if missing:
        raise ValueError(f"{label} is missing required columns: {missing}")


def _pickup_period_condition(column: str, rules: dict):
    periods = [(rules["pickup_start"], rules["pickup_end_exclusive"])]
    periods.extend(rules.get("additional_pickup_periods", []))
    condition = F.lit(False)
    for start, end in periods:
        condition = condition | (
            (F.col(column) >= F.lit(start).cast("timestamp_ntz"))
            & (F.col(column) < F.lit(end).cast("timestamp_ntz"))
        )
    return condition


def _with_record_hash(df: DataFrame) -> DataFrame:
    data_columns = [column for column in df.columns if not column.startswith("_")]
    return df.withColumn(
        "_record_hash",
        F.sha2(F.concat_ws("||", *[F.col(column).cast("string") for column in data_columns]), 256),
    )


def _clean_raw_taxi(
    df: DataFrame,
    machine_learning: dict,
    dataset_config: dict,
) -> DataFrame:
    """Reproduce Week 1 taxi cleaning directly from original files."""
    required = (
        "pickup_timestamp", "dropoff_timestamp", "pickup_location_id",
        "dropoff_location_id", "trip_distance",
    )
    _require_columns(df, required, "Raw taxi source")
    rules = dataset_config["quality_rules"]
    max_duration = rules["max_trip_duration_seconds"]
    max_distance = rules["max_trip_distance"]

    deduped = _with_record_hash(df).dropDuplicates(["_record_hash"])
    selected = deduped.select(
        F.col("_record_hash").alias("trip_id"),
        F.col("pickup_timestamp").cast("timestamp_ntz").alias("pickup_timestamp"),
        F.col("dropoff_timestamp").cast("timestamp_ntz").alias("dropoff_timestamp"),
        F.col("pickup_location_id").cast("int").alias("pickup_location_id"),
        F.col("dropoff_location_id").cast("int").alias("dropoff_location_id"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
    ).withColumn(
        "trip_duration_seconds",
        F.expr("timestampdiff(SECOND, pickup_timestamp, dropoff_timestamp)"),
    )

    pickup_present = F.col("pickup_timestamp").isNotNull()
    timestamps_present = pickup_present & F.col("dropoff_timestamp").isNotNull()
    in_period = _pickup_period_condition("pickup_timestamp", rules)
    cleaned = selected.filter(
        F.col("trip_id").isNotNull()
        & F.col("pickup_timestamp").isNotNull()
        & F.col("dropoff_timestamp").isNotNull()
        & F.col("pickup_location_id").isNotNull()
        & F.col("dropoff_location_id").isNotNull()
        & F.col("trip_distance").isNotNull()
        & ~(pickup_present & ~in_period)
        & ~(timestamps_present & F.col("trip_duration_seconds").isNull())
        & ~(
            F.col("trip_duration_seconds").isNotNull()
            & ((F.col("trip_duration_seconds") <= 0) | (F.col("trip_duration_seconds") > max_duration))
        )
        & ~(
            F.col("trip_distance").isNotNull()
            & ~F.isnan(F.col("trip_distance"))
            & ~F.col("trip_distance").between(0, max_distance)
        )
    )

    # Match the Week 1 platform semantics observed in the integrated table:
    # NaN distance values are not treated as out-of-range trip-level rejects.
    # They survive the trip row but are normalized to null before downstream use.
    cleaned = cleaned.withColumn(
        "trip_distance",
        F.when(
            F.isnan(F.col("trip_distance")),
            F.lit(None).cast("double"),
        ).otherwise(F.col("trip_distance")),
    )

    return (
        cleaned.withColumn(
            "pickup_hour",
            F.make_timestamp_ntz(
                F.year("pickup_timestamp"), F.month("pickup_timestamp"),
                F.dayofmonth("pickup_timestamp"), F.hour("pickup_timestamp"),
                F.lit(0), F.lit(0),
            ),
        )
        .select(
            "trip_id",
            "pickup_timestamp",
            "dropoff_timestamp",
            "pickup_hour",
            "pickup_location_id",
            "dropoff_location_id",
            "trip_distance",
            "trip_duration_seconds",
        )
    )


def _clean_raw_zones(df: DataFrame) -> DataFrame:
    required = ("location_id", "borough", "zone", "service_zone")
    _require_columns(df, required, "Raw taxi-zone source")
    latest = df.filter(F.col("location_id").isNotNull()).dropDuplicates(["location_id"])
    accepted = latest.filter(
        F.col("borough").isNotNull() & (F.trim(F.col("borough")) != "")
        & F.col("zone").isNotNull() & (F.trim(F.col("zone")) != "")
        & F.col("service_zone").isNotNull() & (F.trim(F.col("service_zone")) != "")
    )
    return accepted.select(
        F.col("location_id").cast("int").alias("pickup_location_id"),
        F.trim(F.col("zone")).alias("pickup_zone"),
        F.trim(F.col("borough")).alias("pickup_borough"),
        F.trim(F.col("service_zone")).alias("pickup_service_zone"),
    )


def _optional_double(df: DataFrame, name: str):
    if name in df.columns:
        return F.col(name).cast("double")
    return F.lit(None).cast("double")


def _clean_raw_weather(df: DataFrame) -> DataFrame:
    required = ("year", "month", "day", "hour")
    _require_columns(df, required, "Raw weather source")
    latest = (
        df.filter(
            F.col("year").isNotNull() & F.col("month").isNotNull()
            & F.col("day").isNotNull() & F.col("hour").isNotNull()
        )
        .dropDuplicates(["year", "month", "day", "hour"])
    )
    weather = (
        latest.select(
            F.col("year").cast("int").alias("year"),
            F.col("month").cast("int").alias("month"),
            F.col("day").cast("int").alias("day"),
            F.col("hour").cast("int").alias("hour"),
            _optional_double(latest, "temp").alias("temperature_c"),
            _optional_double(latest, "rhum").alias("relative_humidity_pct"),
            _optional_double(latest, "prcp").alias("precipitation_mm"),
            _optional_double(latest, "snwd").alias("snow_depth_mm"),
            _optional_double(latest, "wdir").alias("wind_direction_deg"),
            _optional_double(latest, "wspd").alias("wind_speed_kmh"),
            _optional_double(latest, "wpgt").alias("wind_gust_kmh"),
            _optional_double(latest, "pres").alias("pressure_hpa"),
            _optional_double(latest, "cldc").alias("cloud_cover_pct"),
            (F.col("coco").cast("int") if "coco" in latest.columns else F.lit(None).cast("int")).alias("weather_condition_code"),
            _optional_double(latest, "humidity").alias("humidity"),
        )
        .withColumn(
            "pickup_hour",
            F.make_timestamp_ntz("year", "month", "day", "hour", F.lit(0), F.lit(0)),
        )
    )
    invalid = (
        F.col("pickup_hour").isNull()
        | (F.col("relative_humidity_pct").isNotNull() & ~F.col("relative_humidity_pct").between(0, 100))
        | (F.col("precipitation_mm").isNotNull() & (F.col("precipitation_mm") < 0))
        | (F.col("snow_depth_mm").isNotNull() & (F.col("snow_depth_mm") < 0))
        | (F.col("wind_direction_deg").isNotNull() & ~F.col("wind_direction_deg").between(0, 360))
        | (F.col("wind_speed_kmh").isNotNull() & (F.col("wind_speed_kmh") < 0))
        | (F.col("wind_gust_kmh").isNotNull() & (F.col("wind_gust_kmh") < 0))
        | (F.col("cloud_cover_pct").isNotNull() & ~F.col("cloud_cover_pct").between(0, 100))
        | (F.col("humidity").isNotNull() & ~F.col("humidity").between(0, 100))
    )
    return weather.filter(~invalid).select(
        "pickup_hour", "temperature_c", "relative_humidity_pct", "precipitation_mm",
        "snow_depth_mm", "wind_direction_deg", "wind_speed_kmh", "wind_gust_kmh",
        "pressure_hpa", "cloud_cover_pct", "weather_condition_code",
    )


def _normalize_air_times(df: DataFrame) -> DataFrame:
    for column, date_column in [("time_local", "date_local"), ("time_gmt", "date_gmt")]:
        if column in df.columns and date_column in df.columns:
            value = F.regexp_extract(F.col(column), r"(\d{1,2}:\d{2}(?::\d{2})?)$", 1)
            parsed = F.to_timestamp_ntz(F.concat_ws(" ", F.col(date_column).cast("string"), value))
            df = df.withColumn(
                column,
                F.coalesce(F.substring(parsed.cast("string"), 12, 8), F.col(column).cast("string")),
            )
    return df


def _clean_raw_air_quality(df: DataFrame) -> DataFrame:
    required = (
        "state_name", "county_name", "date_local", "time_local",
        "sample_measurement", "units_of_measure",
    )
    _require_columns(df, required, "Raw air-quality source")
    df = _normalize_air_times(df)
    nyc_counties = ("Bronx", "Kings", "Queens")
    expected_unit = "Micrograms/cubic meter (LC)"
    key_columns = [
        "state_code", "county_code", "site_num", "parameter_code", "poc",
        "date_local", "time_local",
    ]
    _require_columns(df, key_columns, "Raw air-quality source")
    latest = (
        df.filter((F.col("state_name") == "New York") & F.col("county_name").isin(*nyc_counties))
        .filter(
            F.col("state_code").isNotNull() & F.col("county_code").isNotNull()
            & F.col("site_num").isNotNull() & F.col("parameter_code").isNotNull()
            & F.col("poc").isNotNull() & F.col("date_local").isNotNull()
            & F.col("time_local").isNotNull()
        )
        .dropDuplicates(key_columns)
    )
    selected = (
        latest.select(
            "county_code", "site_num", "poc", "date_local", "time_local",
            F.col("sample_measurement").cast("double").alias("pm25_ug_m3"),
            "units_of_measure",
            _optional_double(latest, "aqi").alias("aqi"),
        )
        .withColumn(
            "pickup_hour",
            F.to_timestamp_ntz(
                F.concat_ws(
                    " ", F.date_format("date_local", "yyyy-MM-dd"),
                    F.regexp_extract(F.col("time_local").cast("string"), r"(\d{1,2}:\d{2}(?::\d{2})?)$", 1),
                )
            ),
        )
    )
    valid = selected.filter(
        F.col("pickup_hour").isNotNull()
        & F.col("pm25_ug_m3").isNotNull()
        & (F.col("pm25_ug_m3") >= 0)
        & ~(F.col("aqi").isNotNull() & ~F.col("aqi").between(0, 500))
        & F.col("units_of_measure").isNotNull()
        & (F.col("units_of_measure") == expected_unit)
    )
    return valid.groupBy("pickup_hour").agg(
        F.avg("pm25_ug_m3").alias("pm25_avg_ug_m3"),
        F.min("pm25_ug_m3").alias("pm25_min_ug_m3"),
        F.max("pm25_ug_m3").alias("pm25_max_ug_m3"),
        F.count(F.lit(1)).cast("long").alias("air_quality_observation_count"),
        F.countDistinct("county_code", "site_num").cast("long").alias("air_quality_site_count"),
    )


def build_approach_a_integrated(
    spark: SparkSession,
    config: dict,
) -> DataFrame:
    """Build the ML-required trip-grain input directly from original files."""
    machine_learning = config["machine_learning"]

    taxi = _clean_raw_taxi(
        _read_original_source(spark, config, "yellow_taxi_trips"),
        machine_learning,
        config["datasets"]["yellow_taxi_trips"],
    )
    zones = _clean_raw_zones(
        _read_original_source(spark, config, "taxi_zone_lookup")
    )
    weather = _clean_raw_weather(
        _read_original_source(spark, config, "weather_hourly")
    )
    air_quality = _clean_raw_air_quality(
        _read_original_source(spark, config, "air_quality")
    )

    # Week 1 rejects trips when either the pickup or dropoff location is absent
    # from the taxi-zone lookup.  Approach A reproduces that rule explicitly.
    valid_dropoff_zones = F.broadcast(
        zones.select(
            F.col("pickup_location_id").alias("dropoff_location_id")
        ).dropDuplicates(["dropoff_location_id"])
    )

    integrated = (
        taxi.join(
            valid_dropoff_zones,
            on="dropoff_location_id",
            how="inner",
        )
        .join(F.broadcast(zones), on="pickup_location_id", how="inner")
        .join(F.broadcast(weather), on="pickup_hour", how="left")
        .join(F.broadcast(air_quality), on="pickup_hour", how="left")
        .withColumn("weather_available", F.col("temperature_c").isNotNull())
        .withColumn(
            "air_quality_available",
            F.col("pm25_avg_ug_m3").isNotNull(),
        )
    )
    return integrated


def build_approach_a_dataset(
    spark: SparkSession,
    config: dict,
) -> DataFrame:
    integrated = build_approach_a_integrated(spark, config)
    return hourly_taxi_demand_dataset(integrated, config["machine_learning"])


def build_approach_b_dataset(
    spark: SparkSession,
    config: dict,
) -> DataFrame:
    """Reuse the Weeks 1-3 integrated Delta table, as required by Approach B."""
    machine_learning = config["machine_learning"]
    integrated = read_delta(
        spark,
        table_path(config, machine_learning["source_table"]),
    )
    return hourly_taxi_demand_dataset(integrated, machine_learning)


def _materialize(df: DataFrame) -> int:
    """Persist and force Spark execution so measured time is not just lazy setup."""
    df.cache()
    return df.count()


def _dataset_summary(dataset: DataFrame) -> dict:
    row = dataset.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum("demand").cast("long").alias("demand_sum"),
        F.sum((F.col("demand") == 0).cast("long")).alias("zero_demand_rows"),
        F.countDistinct("pickup_location_id").alias("distinct_zones"),
        F.min("pickup_hour").alias("first_hour"),
        F.max("pickup_hour").alias("last_hour"),
    ).first()
    split_counts = {
        item["split"]: item["count"]
        for item in dataset.groupBy("split").count().collect()
    }
    return {
        "rows": int(row["rows"] or 0),
        "train_rows": int(split_counts.get("train", 0)),
        "validation_rows": int(split_counts.get("validation", 0)),
        "test_rows": int(split_counts.get("test", 0)),
        "demand_sum": int(row["demand_sum"] or 0),
        "zero_demand_rows": int(row["zero_demand_rows"] or 0),
        "distinct_zones": int(row["distinct_zones"] or 0),
        "first_hour": str(row["first_hour"]) if row["first_hour"] is not None else None,
        "last_hour": str(row["last_hour"]) if row["last_hour"] is not None else None,
    }


FLOAT_ABS_TOLERANCE = 1e-9
FLOAT_REL_TOLERANCE = 1e-9


def _float_equivalent(left, right):
    """Null/NaN-safe floating-point comparison used only for validation.

    Spark aggregations over doubles may differ in the last few binary digits
    when the physical aggregation order changes.  Those differences are not
    semantic data differences, so validation uses a small absolute/relative
    tolerance while categorical/integer/timestamp columns remain exact.
    """
    both_null = left.isNull() & right.isNull()
    one_null = left.isNull() != right.isNull()
    both_nan = F.isnan(left) & F.isnan(right)
    one_nan = F.isnan(left) != F.isnan(right)
    scale = F.greatest(F.abs(left), F.abs(right), F.lit(1.0))
    close = F.abs(left - right) <= (
        F.lit(FLOAT_ABS_TOLERANCE)
        + F.lit(FLOAT_REL_TOLERANCE) * scale
    )
    return both_null | (~one_null & both_nan) | (~one_null & ~one_nan & close)


def _column_equivalent(df: DataFrame, column: str):
    left = F.col(f"a__{column}")
    right = F.col(f"b__{column}")
    field = df.schema[f"a__{column}"]
    if isinstance(field.dataType, (DoubleType, FloatType, DecimalType)):
        return _float_equivalent(left.cast("double"), right.cast("double"))
    return left.eqNullSafe(right)


def verify_comparable_datasets(
    a: DataFrame,
    b: DataFrame,
    *,
    exact: bool,
) -> dict:
    """Verify the Task 1 comparison contract.

    ``exact_equal`` is retained as a bit-for-bit multiset check.  Because Spark
    ``avg(double)`` may legitimately differ by ~1e-14 when aggregation order
    changes, ``numerically_equal`` is the semantic comparison used for the
    experiment: floating-point columns use a 1e-9 abs/relative tolerance and
    every other column is still compared exactly.
    """
    schema_equal = a.schema.simpleString() == b.schema.simpleString()
    columns_equal = a.columns == b.columns
    summary_a = _dataset_summary(a)
    summary_b = _dataset_summary(b)
    summary_equal = summary_a == summary_b

    exact_equal: bool | None = None
    numerically_equal: bool | None = None
    tolerant_mismatch_rows: int | None = None

    if exact and schema_equal and columns_equal:
        a_only = a.exceptAll(b).limit(1).count()
        b_only = b.exceptAll(a).limit(1).count()
        exact_equal = a_only == 0 and b_only == 0

        keys = ["pickup_hour", "pickup_location_id"]
        comparable = [c for c in a.columns if c not in keys]
        a_pref = a.select(*keys, *[F.col(c).alias(f"a__{c}") for c in comparable])
        b_pref = b.select(*keys, *[F.col(c).alias(f"b__{c}") for c in comparable])
        joined = a_pref.join(b_pref, keys, "full")

        mismatch = None
        for c in comparable:
            equal_expr = _column_equivalent(joined, c)
            this_mismatch = ~equal_expr
            mismatch = this_mismatch if mismatch is None else (mismatch | this_mismatch)

        tolerant_mismatch_rows = int(joined.filter(mismatch).count()) if mismatch is not None else 0
        numerically_equal = tolerant_mismatch_rows == 0

    return {
        "schema_equal": schema_equal,
        "columns_equal": columns_equal,
        "summary_equal": summary_equal,
        "exact_equal": exact_equal,
        "numerically_equal": numerically_equal,
        "float_abs_tolerance": FLOAT_ABS_TOLERANCE,
        "float_rel_tolerance": FLOAT_REL_TOLERANCE,
        "tolerant_mismatch_rows": tolerant_mismatch_rows,
        "approach_a": summary_a,
        "approach_b": summary_b,
    }


def write_context_feature_diagnostics(
    a: DataFrame,
    b: DataFrame,
    output_dir: Path,
) -> dict:
    """Compare non-key Task 1 columns with exact and numeric-tolerant checks.

    Exact floating-point drift is reported for transparency, but only values
    outside the configured numeric tolerance are treated as semantic feature
    mismatches.
    """
    keys = ["pickup_hour", "pickup_location_id"]
    comparable = [c for c in a.columns if c in b.columns and c not in keys]

    a_pref = a.select(*keys, *[F.col(c).alias(f"a__{c}") for c in comparable])
    b_pref = b.select(*keys, *[F.col(c).alias(f"b__{c}") for c in comparable])
    joined = a_pref.join(b_pref, keys, "inner").cache()
    joined.count()

    strict_counts: dict[str, int] = {}
    tolerant_counts: dict[str, int] = {}
    max_abs_float_diff: dict[str, float] = {}

    for c in comparable:
        strict = ~F.col(f"a__{c}").eqNullSafe(F.col(f"b__{c}"))
        strict_counts[c] = int(joined.filter(strict).count())

        equivalent = _column_equivalent(joined, c)
        tolerant_counts[c] = int(joined.filter(~equivalent).count())

        field = joined.schema[f"a__{c}"]
        if isinstance(field.dataType, (DoubleType, FloatType, DecimalType)):
            row = joined.select(
                F.max(
                    F.abs(
                        F.col(f"a__{c}").cast("double")
                        - F.col(f"b__{c}").cast("double")
                    )
                ).alias("max_abs")
            ).first()
            if row["max_abs"] is not None:
                max_abs_float_diff[c] = float(row["max_abs"])

    strict_differing_columns = {k: v for k, v in strict_counts.items() if v > 0}
    differing_columns = {k: v for k, v in tolerant_counts.items() if v > 0}

    # Export only semantic mismatches (outside tolerance).  This keeps the CSV
    # useful while the JSON still records harmless strict float drift.
    mismatch_structs = []
    for c in comparable:
        cond = ~_column_equivalent(joined, c)
        mismatch_structs.append(
            F.when(
                cond,
                F.struct(
                    F.lit(c).alias("column"),
                    F.col(f"a__{c}").cast("string").alias("a_value"),
                    F.col(f"b__{c}").cast("string").alias("b_value"),
                ),
            )
        )

    long_df = (
        joined
        .select(*keys, F.explode(F.array(*mismatch_structs)).alias("diff"))
        .filter(F.col("diff").isNotNull())
        .select(
            *keys,
            F.col("diff.column").alias("column"),
            F.col("diff.a_value").alias("a_value"),
            F.col("diff.b_value").alias("b_value"),
        )
        .orderBy("column", *keys)
    )

    csv_path = output_dir / "context_feature_differences.csv"
    rows = long_df.collect()
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["pickup_hour", "pickup_location_id", "column", "a_value", "b_value"],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({
                "pickup_hour": str(row["pickup_hour"]),
                "pickup_location_id": row["pickup_location_id"],
                "column": row["column"],
                "a_value": row["a_value"],
                "b_value": row["b_value"],
            })

    summary = {
        "compared_columns": comparable,
        "strict_differing_columns": strict_differing_columns,
        "differing_columns_after_float_tolerance": differing_columns,
        "strict_total_cell_mismatches": int(sum(strict_differing_columns.values())),
        "semantic_total_cell_mismatches": int(sum(differing_columns.values())),
        "float_abs_tolerance": FLOAT_ABS_TOLERANCE,
        "float_rel_tolerance": FLOAT_REL_TOLERANCE,
        "max_abs_float_difference": max_abs_float_diff,
        "csv": str(csv_path),
    }
    with (output_dir / "context_feature_difference_summary.json").open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, ensure_ascii=False)

    print("\n=== Context feature differences (A vs B) ===")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    joined.unpersist()
    return summary


def write_demand_diagnostics(
    a: DataFrame,
    b: DataFrame,
    output_dir: Path,
) -> dict:
    """Write zone-hour demand differences between Approach A and B.

    The Task 1 datasets contain one row per pickup_hour + pickup_location_id.
    This diagnostic intentionally compares only the demand column so contextual
    feature differences do not hide the source of the remaining trip-count gap.
    """
    a_demand = a.select(
        "pickup_hour",
        "pickup_location_id",
        "split",
        F.col("demand").cast("long").alias("a_demand"),
    )
    b_demand = b.select(
        "pickup_hour",
        "pickup_location_id",
        "split",
        F.col("demand").cast("long").alias("b_demand"),
    )

    diff = (
        a_demand.alias("a")
        .join(
            b_demand.alias("b"),
            on=["pickup_hour", "pickup_location_id"],
            how="full",
        )
        .select(
            "pickup_hour",
            "pickup_location_id",
            F.coalesce(F.col("a.split"), F.col("b.split")).alias("split"),
            F.coalesce(F.col("a.a_demand"), F.lit(0)).cast("long").alias("a_demand"),
            F.coalesce(F.col("b.b_demand"), F.lit(0)).cast("long").alias("b_demand"),
        )
        .withColumn("difference", F.col("a_demand") - F.col("b_demand"))
        .withColumn("abs_difference", F.abs(F.col("difference")))
        .filter(F.col("difference") != 0)
        .orderBy(
            F.desc("abs_difference"),
            F.asc("pickup_hour"),
            F.asc("pickup_location_id"),
        )
        .cache()
    )

    rows = diff.collect()
    csv_path = output_dir / "demand_difference.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "pickup_hour",
                "pickup_location_id",
                "split",
                "a_demand",
                "b_demand",
                "difference",
                "abs_difference",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({
                "pickup_hour": str(row["pickup_hour"]),
                "pickup_location_id": int(row["pickup_location_id"]),
                "split": row["split"],
                "a_demand": int(row["a_demand"]),
                "b_demand": int(row["b_demand"]),
                "difference": int(row["difference"]),
                "abs_difference": int(row["abs_difference"]),
            })

    signed_difference = sum(int(row["difference"]) for row in rows)
    absolute_difference = sum(int(row["abs_difference"]) for row in rows)
    summary = {
        "different_zone_hour_rows": len(rows),
        "signed_a_minus_b_demand": signed_difference,
        "absolute_demand_difference": absolute_difference,
        "approach_a_total_minus_b_total": signed_difference,
        "csv": str(csv_path),
    }
    with (output_dir / "demand_difference_summary.json").open(
        "w", encoding="utf-8"
    ) as handle:
        json.dump(summary, handle, indent=2, ensure_ascii=False)

    print("\n=== Demand differences (A vs B) ===")
    print(json.dumps(summary, indent=2))
    if rows:
        diff.show(min(100, len(rows)), truncate=False)
    diff.unpersist()
    return summary


def _raw_taxi_rule_diagnostics(
    spark: SparkSession,
    config: dict,
) -> DataFrame:
    """Return one direct-raw taxi row per recreated trip_id with A-rule flags.

    This does not decide that Approach A is correct.  It only explains which
    standalone rule would reject a trip that the Weeks 1-3 integrated table kept.
    That distinction matters because Week 1 raw Delta also has ingestion lineage
    (_ingestion_timestamp and _schema_version) that original files do not expose.
    """
    dataset_config = config["datasets"]["yellow_taxi_trips"]
    rules = dataset_config["quality_rules"]
    max_duration = rules["max_trip_duration_seconds"]
    max_distance = rules["max_trip_distance"]

    raw = _read_original_source(spark, config, "yellow_taxi_trips")
    hashed = _with_record_hash(raw).dropDuplicates(["_record_hash"])
    base = (
        hashed.select(
            F.col("_record_hash").alias("trip_id"),
            F.col("pickup_timestamp").cast("timestamp_ntz").alias("pickup_timestamp"),
            F.col("dropoff_timestamp").cast("timestamp_ntz").alias("dropoff_timestamp"),
            F.col("pickup_location_id").cast("int").alias("pickup_location_id"),
            F.col("dropoff_location_id").cast("int").alias("dropoff_location_id"),
            F.col("trip_distance").cast("double").alias("trip_distance"),
        )
        .withColumn(
            "trip_duration_seconds",
            F.expr("timestampdiff(SECOND, pickup_timestamp, dropoff_timestamp)"),
        )
    )

    pickup_present = F.col("pickup_timestamp").isNotNull()
    timestamps_present = pickup_present & F.col("dropoff_timestamp").isNotNull()
    in_period = _pickup_period_condition("pickup_timestamp", rules)

    flagged = (
        base
        .withColumn("failed_pickup_timestamp_required", F.col("pickup_timestamp").isNull())
        .withColumn("failed_dropoff_timestamp_required", F.col("dropoff_timestamp").isNull())
        .withColumn("failed_pickup_location_required", F.col("pickup_location_id").isNull())
        .withColumn("failed_dropoff_location_required", F.col("dropoff_location_id").isNull())
        .withColumn("failed_trip_distance_required", F.col("trip_distance").isNull())
        .withColumn("failed_pickup_period", pickup_present & ~in_period)
        .withColumn(
            "failed_duration_calculation",
            timestamps_present & F.col("trip_duration_seconds").isNull(),
        )
        .withColumn(
            "failed_duration_range",
            F.col("trip_duration_seconds").isNotNull()
            & (
                (F.col("trip_duration_seconds") <= 0)
                | (F.col("trip_duration_seconds") > F.lit(max_duration))
            ),
        )
        .withColumn(
            "failed_distance_range",
            F.col("trip_distance").isNotNull()
            & ~F.isnan(F.col("trip_distance"))
            & ~F.col("trip_distance").between(0, max_distance),
        )
        .withColumn(
            "trip_distance_would_be_normalized_to_null",
            F.isnan(F.col("trip_distance")),
        )
    )

    zones = _clean_raw_zones(_read_original_source(spark, config, "taxi_zone_lookup"))
    pickup_ids = zones.select(
        F.col("pickup_location_id").alias("_valid_pickup_location_id")
    ).dropDuplicates(["_valid_pickup_location_id"])
    dropoff_ids = zones.select(
        F.col("pickup_location_id").alias("_valid_dropoff_location_id")
    ).dropDuplicates(["_valid_dropoff_location_id"])

    flagged = (
        flagged
        .join(
            F.broadcast(pickup_ids),
            flagged["pickup_location_id"] == pickup_ids["_valid_pickup_location_id"],
            "left",
        )
        .join(
            F.broadcast(dropoff_ids),
            flagged["dropoff_location_id"] == dropoff_ids["_valid_dropoff_location_id"],
            "left",
        )
        .withColumn(
            "failed_pickup_reference",
            F.col("pickup_location_id").isNotNull()
            & F.col("_valid_pickup_location_id").isNull(),
        )
        .withColumn(
            "failed_dropoff_reference",
            F.col("dropoff_location_id").isNotNull()
            & F.col("_valid_dropoff_location_id").isNull(),
        )
        .drop("_valid_pickup_location_id", "_valid_dropoff_location_id")
    )

    rule_columns = [
        "failed_pickup_timestamp_required",
        "failed_dropoff_timestamp_required",
        "failed_pickup_location_required",
        "failed_dropoff_location_required",
        "failed_trip_distance_required",
        "failed_pickup_period",
        "failed_duration_calculation",
        "failed_duration_range",
        "failed_distance_range",
        "failed_pickup_reference",
        "failed_dropoff_reference",
    ]
    any_failure = F.lit(False)
    for column in rule_columns:
        any_failure = any_failure | F.col(column)
    return flagged.withColumn("failed_any_approach_a_rule", any_failure)


def _approach_a_accepted_trip_ids(
    spark: SparkSession,
    config: dict,
) -> DataFrame:
    """Return trip IDs accepted by the current standalone A taxi/reference path."""
    taxi = _clean_raw_taxi(
        _read_original_source(spark, config, "yellow_taxi_trips"),
        config["machine_learning"],
        config["datasets"]["yellow_taxi_trips"],
    )
    zones = _clean_raw_zones(_read_original_source(spark, config, "taxi_zone_lookup"))
    pickup_ids = zones.select("pickup_location_id").dropDuplicates(["pickup_location_id"])
    dropoff_ids = zones.select(
        F.col("pickup_location_id").alias("dropoff_location_id")
    ).dropDuplicates(["dropoff_location_id"])
    return (
        taxi
        .join(F.broadcast(pickup_ids), "pickup_location_id", "inner")
        .join(F.broadcast(dropoff_ids), "dropoff_location_id", "inner")
        .select("trip_id")
        .dropDuplicates(["trip_id"])
    )


def _write_rows_csv(rows: list, path: Path, fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            item = row.asDict(recursive=True)
            for key, value in list(item.items()):
                if isinstance(value, (datetime,)):
                    item[key] = str(value)
            writer.writerow({key: item.get(key) for key in fieldnames})



def write_trip_level_diagnostics(
    spark: SparkSession,
    config: dict,
    output_dir: Path,
) -> dict:
    """Export the Q1 A/B trip difference and show the exact A stage where each B-only trip disappears.

    This diagnostic intentionally restricts Approach B to the same ML observation
    window as Task 1.  That removes Week 3 incremental trips outside Q1 and leaves
    only the records relevant to the Week 4 comparison.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    ml = config["machine_learning"]

    # ---------- Approach B: only the observation window used by Task 1 ----------
    b_integrated_all = read_delta(spark, table_path(config, ml["source_table"]))
    if "trip_id" not in b_integrated_all.columns:
        raise ValueError(
            "Integrated Delta table has no trip_id; trip-level diagnostics require "
            "the Week 1 trip_id lineage column."
        )

    b_integrated = filter_observation_period(b_integrated_all, ml)
    b_fields = [
        "trip_id",
        "pickup_timestamp",
        "dropoff_timestamp",
        "pickup_hour",
        "pickup_location_id",
        "dropoff_location_id",
        "trip_distance",
        "trip_duration_seconds",
    ]
    available_b_fields = [c for c in b_fields if c in b_integrated.columns]
    b_trips = (
        b_integrated
        .select(*available_b_fields)
        .dropDuplicates(["trip_id"])
        .cache()
    )
    b_count = b_trips.count()
    b_ids = b_trips.select("trip_id").dropDuplicates(["trip_id"]).cache()

    # ---------- Recreate each important Approach A stage ----------
    raw_taxi = _read_original_source(spark, config, "yellow_taxi_trips")

    # Same record-hash construction used by the standalone Approach A.
    raw_hashed = _with_record_hash(raw_taxi)
    raw_dedup = raw_hashed.dropDuplicates(["_record_hash"])

    raw_stage = (
        raw_dedup
        .select(
            F.col("_record_hash").alias("trip_id"),
            F.col("pickup_timestamp").cast("timestamp_ntz").alias("raw_pickup_timestamp"),
            F.col("dropoff_timestamp").cast("timestamp_ntz").alias("raw_dropoff_timestamp"),
            F.col("pickup_location_id").cast("int").alias("raw_pickup_location_id"),
            F.col("dropoff_location_id").cast("int").alias("raw_dropoff_location_id"),
            F.col("trip_distance").cast("double").alias("raw_trip_distance"),
        )
        .withColumn("present_after_raw_hash_dedup", F.lit(True))
        .withColumn(
            "raw_trip_distance_is_nan",
            F.isnan(F.col("raw_trip_distance")),
        )
        .cache()
    )
    raw_stage.count()

    # Taxi-validity stage, including the current NaN handling.
    cleaned_taxi = _clean_raw_taxi(
        raw_taxi,
        ml,
        config["datasets"]["yellow_taxi_trips"],
    ).cache()
    cleaned_taxi.count()

    taxi_stage = (
        cleaned_taxi
        .select("trip_id")
        .dropDuplicates(["trip_id"])
        .withColumn("present_after_taxi_cleaning", F.lit(True))
    )

    # Reference stages are separated so we can tell pickup and dropoff failures apart.
    zones = _clean_raw_zones(
        _read_original_source(spark, config, "taxi_zone_lookup")
    )
    pickup_ids = zones.select("pickup_location_id").dropDuplicates(["pickup_location_id"])
    dropoff_ids = zones.select(
        F.col("pickup_location_id").alias("dropoff_location_id")
    ).dropDuplicates(["dropoff_location_id"])

    after_pickup = (
        cleaned_taxi
        .join(F.broadcast(pickup_ids), "pickup_location_id", "inner")
        .select("trip_id")
        .dropDuplicates(["trip_id"])
        .withColumn("present_after_pickup_reference", F.lit(True))
    )
    after_dropoff = (
        cleaned_taxi
        .join(F.broadcast(pickup_ids), "pickup_location_id", "inner")
        .join(F.broadcast(dropoff_ids), "dropoff_location_id", "inner")
        .select("trip_id")
        .dropDuplicates(["trip_id"])
        .withColumn("present_after_dropoff_reference", F.lit(True))
        .cache()
    )
    a_count = after_dropoff.count()
    a_ids = after_dropoff.select("trip_id")

    # Compare only Q1 Task-1-relevant trip IDs.
    b_only_ids = b_ids.join(a_ids, "trip_id", "left_anti").cache()
    a_only_ids = a_ids.join(b_ids, "trip_id", "left_anti").cache()
    b_only_count = b_only_ids.count()
    a_only_count = a_only_ids.count()

    # Keep the existing rule-level explanation, but also show exact stage membership.
    rule_flags = _raw_taxi_rule_diagnostics(spark, config).cache()
    rule_flags.count()

    diagnostic_columns = [
        "trip_id",
        "failed_pickup_timestamp_required",
        "failed_dropoff_timestamp_required",
        "failed_pickup_location_required",
        "failed_dropoff_location_required",
        "failed_trip_distance_required",
        "failed_pickup_period",
        "failed_duration_calculation",
        "failed_duration_range",
        "failed_distance_range",
        "failed_pickup_reference",
        "failed_dropoff_reference",
        "failed_any_approach_a_rule",
    ]

    b_only = (
        b_trips
        .join(b_only_ids, "trip_id", "inner")
        .join(raw_stage, "trip_id", "left")
        .join(taxi_stage, "trip_id", "left")
        .join(after_pickup, "trip_id", "left")
        .join(after_dropoff, "trip_id", "left")
        .join(rule_flags.select(*diagnostic_columns), "trip_id", "left")
        .withColumn(
            "present_after_raw_hash_dedup",
            F.coalesce(F.col("present_after_raw_hash_dedup"), F.lit(False)),
        )
        .withColumn(
            "present_after_taxi_cleaning",
            F.coalesce(F.col("present_after_taxi_cleaning"), F.lit(False)),
        )
        .withColumn(
            "present_after_pickup_reference",
            F.coalesce(F.col("present_after_pickup_reference"), F.lit(False)),
        )
        .withColumn(
            "present_after_dropoff_reference",
            F.coalesce(F.col("present_after_dropoff_reference"), F.lit(False)),
        )
        .withColumn(
            "disappears_at_stage",
            F.when(
                ~F.col("present_after_raw_hash_dedup"),
                F.lit("raw_hash_reproduction"),
            )
            .when(
                ~F.col("present_after_taxi_cleaning"),
                F.lit("taxi_cleaning"),
            )
            .when(
                ~F.col("present_after_pickup_reference"),
                F.lit("pickup_reference"),
            )
            .when(
                ~F.col("present_after_dropoff_reference"),
                F.lit("dropoff_reference"),
            )
            .otherwise(F.lit("not_missing_from_A_trip_path")),
        )
        .orderBy("pickup_hour", "pickup_location_id", "trip_id")
    )

    a_only = a_only_ids.orderBy("trip_id")

    b_only_rows = b_only.collect()
    a_only_rows = a_only.collect()
    _write_rows_csv(
        b_only_rows,
        output_dir / "b_only_trips.csv",
        b_only.columns,
    )
    _write_rows_csv(
        a_only_rows,
        output_dir / "a_only_trips.csv",
        a_only.columns,
    )

    # Aggregate stage disappearance and rule evidence.
    stage_counts: dict[str, int] = {}
    failure_columns = [
        c for c in diagnostic_columns
        if c.startswith("failed_") and c != "failed_any_approach_a_rule"
    ]
    failure_counts: dict[str, int] = {}
    nan_count = 0

    for row in b_only_rows:
        item = row.asDict(recursive=True)
        stage = item.get("disappears_at_stage") or "unknown"
        stage_counts[stage] = stage_counts.get(stage, 0) + 1
        if item.get("raw_trip_distance_is_nan") is True:
            nan_count += 1
        for column in failure_columns:
            if item.get(column) is True:
                failure_counts[column] = failure_counts.get(column, 0) + 1

    summary = {
        "observation_window_only": True,
        "approach_a_accepted_trip_ids": a_count,
        "approach_b_q1_integrated_trip_ids": b_count,
        "b_only_trip_ids": b_only_count,
        "a_only_trip_ids": a_only_count,
        "b_only_stage_counts": stage_counts,
        "b_only_raw_trip_distance_nan_count": nan_count,
        "b_only_failure_rule_counts": failure_counts,
        "b_only_csv": str(output_dir / "b_only_trips.csv"),
        "a_only_csv": str(output_dir / "a_only_trips.csv"),
        "interpretation_note": (
            "For each B-only Q1 trip, disappears_at_stage identifies the first "
            "standalone Approach A stage that no longer contains that trip. "
            "This is more reliable than inferring the cause from aggregate demand."
        ),
    }

    with (output_dir / "trip_difference_summary.json").open(
        "w", encoding="utf-8"
    ) as handle:
        json.dump(summary, handle, indent=2, ensure_ascii=False)

    print("\n=== Q1 trip-level A/B diagnostics ===")
    print(json.dumps(summary, indent=2))
    if b_only_rows:
        print("\nFirst B-only Q1 trips with stage diagnostics:")
        b_only.show(min(100, len(b_only_rows)), truncate=False)

    raw_stage.unpersist()
    cleaned_taxi.unpersist()
    after_dropoff.unpersist()
    b_trips.unpersist()
    b_ids.unpersist()
    b_only_ids.unpersist()
    a_only_ids.unpersist()
    rule_flags.unpersist()

    return summary


def _fit_shared_features(dataset: DataFrame, config: dict) -> tuple[DataFrame, float]:
    """Fit and apply the shared Task 2 feature pipeline, measuring its runtime."""
    started = time.perf_counter()
    model = fit_feature_pipeline(dataset, config)
    prepared = transform_feature_dataset(dataset, model, config).cache()
    prepared.count()
    return prepared, time.perf_counter() - started


def _train_shared_model(prepared: DataFrame, config: dict) -> TrainingResult:
    """Fit the same Random Forest as Task 3 and measure model-fit time only.

    Task 4 keeps feature-engineering time separate, so it cannot directly call
    Task 3's fit_training_pipeline(), which fits preprocessing and regression
    together. The regressor parameters and deterministic train partitioning
    below therefore mirror src.machine_learning.training exactly.
    """
    ml = config["machine_learning"]
    training_config = ml["training"]
    keys = ("pickup_hour", "pickup_location_id")

    train = (
        prepared.filter(F.col("split") == "train")
        .repartition(int(training_config["fit_partitions"]), *keys)
        .sortWithinPartitions(*keys)
        .cache()
    )
    train.count()

    regressor = RandomForestRegressor(
        labelCol=ml["label_column"],
        featuresCol=ml["features_column"],
        predictionCol="prediction",
        seed=int(training_config["seed"]),
        numTrees=int(training_config["num_trees"]),
        maxDepth=int(training_config["max_depth"]),
        maxBins=int(training_config["max_bins"]),
    )

    started = time.perf_counter()
    model = regressor.fit(train)
    seconds = time.perf_counter() - started

    # Sanity-check that the fitted model can score the held-out test split.
    # Keep this action outside the measured fit time because Task 4 reports
    # model training time separately from prediction/evaluation work.
    prediction_rows = (
        model.transform(prepared.filter(F.col("split") == "test"))
        .select("prediction")
        .count()
    )
    train.unpersist()
    return TrainingResult(seconds=seconds, prediction_rows=prediction_rows)

def _run_one(
    spark: SparkSession,
    config: dict,
    approach: str,
    repeat: int,
    order: str,
) -> tuple[RunMetrics, DataFrame]:
    build = build_approach_a_dataset if approach == "A" else build_approach_b_dataset

    total_started = time.perf_counter()
    prep_started = time.perf_counter()
    dataset = build(spark, config).cache()
    _materialize(dataset)
    preparation_seconds = time.perf_counter() - prep_started

    summary = _dataset_summary(dataset)
    prepared, feature_seconds = _fit_shared_features(dataset, config)
    training = _train_shared_model(prepared, config)
    total_seconds = time.perf_counter() - total_started

    metrics = RunMetrics(
        repeat=repeat,
        order=order,
        approach=approach,
        preparation_seconds=preparation_seconds,
        feature_seconds=feature_seconds,
        training_seconds=training.seconds,
        total_seconds=total_seconds,
        **summary,
    )
    prepared.unpersist()
    return metrics, dataset


def _median_summary(rows: list[RunMetrics]) -> dict:
    result: dict[str, dict] = {}
    for approach in ("A", "B"):
        selected = [row for row in rows if row.approach == approach]
        result[approach] = {
            "repeats": len(selected),
            "median_preparation_seconds": statistics.median(
                row.preparation_seconds for row in selected
            ),
            "median_feature_seconds": statistics.median(
                row.feature_seconds for row in selected
            ),
            "median_training_seconds": statistics.median(
                row.training_seconds for row in selected
            ),
            "median_total_seconds": statistics.median(
                row.total_seconds for row in selected
            ),
        }
    return result


def _write_outputs(
    output_dir: Path,
    rows: list[RunMetrics],
    verification: dict,
    demand_diagnostics: dict | None = None,
    context_diagnostics: dict | None = None,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = output_dir / "comparison_runs.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        fieldnames = list(asdict(rows[0]).keys())
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))

    summary = {
        "task": "Week 4 Task 4 - Evaluate the Role of Data Engineering",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "prediction_problem": "hourly taxi demand",
        "fairness_controls": {
            "same_task1_function": True,
            "same_feature_pipeline": True,
            "same_regressor": "Spark MLlib RandomForestRegressor",
            "same_chronological_splits": True,
        },
        "implementation_complexity": {
            "approach_a_raw_sources": len(RAW_DATASETS),
            "approach_b_preintegrated_sources": 1,
            "approach_a_explicit_preprocessing_stages": len(APPROACH_A_STAGES),
            "approach_b_explicit_preprocessing_stages": len(APPROACH_B_STAGES),
            "approach_a_stages": list(APPROACH_A_STAGES),
            "approach_b_stages": list(APPROACH_B_STAGES),
        },
        "reproducibility_evidence": {
            "approach_a": [
                "original file paths and schemas come from version-controlled config",
                "chronological splits are deterministic",
                "shared Task 2 feature pipeline is fitted on train only",
            ],
            "approach_b": [
                "integrated Delta table has a stable platform contract",
                "Weeks 1-3 centralize validation and integration logic",
                "chronological splits are deterministic",
                "shared Task 2 feature pipeline is fitted on train only",
            ],
        },
        "dataset_verification": verification,
        "demand_difference_diagnostics": demand_diagnostics,
        "context_feature_diagnostics": context_diagnostics,
        "timing_medians": _median_summary(rows),
    }
    with (output_dir / "comparison_summary.json").open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, ensure_ascii=False)

    return output_dir


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/config.yaml"),
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=3,
        help="Number of A/B repetitions; use 1 for a smoke run, 3 for report evidence.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("data/evaluation/week4/task4_comparison"),
    )
    parser.add_argument(
        "--exact-check",
        action="store_true",
        help="Also run an expensive exact multiset equality check on Task 1 datasets.",
    )
    parser.add_argument(
        "--trip-diagnostics-only",
        action="store_true",
        help=(
            "Skip ML timing/training and only export A/B trip-level differences "
            "to b_only_trips.csv, a_only_trips.csv, and trip_difference_summary.json."
        ),
    )
    args = parser.parse_args()

    if args.repeats < 1:
        raise ValueError("--repeats must be at least 1")

    config = load_config(args.config)
    if "machine_learning" not in config:
        raise ValueError("config.yaml must contain the Week 4 machine_learning section")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = args.output_root / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    spark = create_spark()
    all_rows: list[RunMetrics] = []
    verification: dict | None = None
    demand_diagnostics: dict | None = None
    context_diagnostics: dict | None = None
    try:
        if args.trip_diagnostics_only:
            write_trip_level_diagnostics(spark, config, output_dir)
            print(f"\nTrip diagnostics written to: {output_dir}")
            print("Inspect b_only_trips.csv and trip_difference_summary.json first.")
            return

        for repeat in range(1, args.repeats + 1):
            # Alternate execution order so one approach is not always first.
            order = "AB" if repeat % 2 == 1 else "BA"
            datasets: dict[str, DataFrame] = {}
            for approach in order:
                print(f"\n=== Repeat {repeat}/{args.repeats}: Approach {approach} ===")
                metrics, dataset = _run_one(
                    spark,
                    config,
                    approach,
                    repeat,
                    order,
                )
                all_rows.append(metrics)
                datasets[approach] = dataset
                print(json.dumps(asdict(metrics), indent=2))

            # Compare once using the first paired run.  The generated Task 1
            # data is deterministic, so repeating equality work only adds cost.
            if verification is None:
                verification = verify_comparable_datasets(
                    datasets["A"],
                    datasets["B"],
                    exact=args.exact_check,
                )
                print("\n=== Dataset comparability ===")
                print(json.dumps(verification, indent=2))
                demand_diagnostics = write_demand_diagnostics(
                    datasets["A"],
                    datasets["B"],
                    output_dir,
                )
                context_diagnostics = write_context_feature_diagnostics(
                    datasets["A"],
                    datasets["B"],
                    output_dir,
                )

            for dataset in datasets.values():
                dataset.unpersist()
            spark.catalog.clearCache()

        assert verification is not None
        output_dir = _write_outputs(
            output_dir,
            all_rows,
            verification,
            demand_diagnostics=demand_diagnostics,
            context_diagnostics=context_diagnostics,
        )
        print(f"\nTask 4 evidence written to: {output_dir}")
        print("Use comparison_runs.csv for the timing table and")
        print("comparison_summary.json for the report discussion/evidence.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
