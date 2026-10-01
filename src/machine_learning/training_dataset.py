"""Build the Week 4 hourly taxi-demand training dataset from Delta tables."""
from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import (
    create_spark,
    load_config,
    read_delta,
    table_path,
    write_delta,
)


LOCATION_COLUMNS = (
    "pickup_location_id",
    "pickup_zone",
    "pickup_borough",
    "pickup_service_zone",
)

NUMERIC_CONTEXT_COLUMNS = (
    "temperature_c",
    "relative_humidity_pct",
    "precipitation_mm",
    "snow_depth_mm",
    "wind_direction_deg",
    "wind_speed_kmh",
    "wind_gust_kmh",
    "pressure_hpa",
    "cloud_cover_pct",
    "weather_condition_code",
    "pm25_avg_ug_m3",
    "pm25_min_ug_m3",
    "pm25_max_ug_m3",
    "air_quality_observation_count",
    "air_quality_site_count",
)

AVAILABILITY_COLUMNS = (
    "weather_available",
    "air_quality_available",
)

REQUIRED_SOURCE_COLUMNS = {
    "pickup_hour",
    *LOCATION_COLUMNS,
    *NUMERIC_CONTEXT_COLUMNS,
    *AVAILABILITY_COLUMNS,
}

OUTPUT_COLUMNS = (
    "pickup_hour",
    *LOCATION_COLUMNS,
    *NUMERIC_CONTEXT_COLUMNS,
    *AVAILABILITY_COLUMNS,
    "demand",
    "split",
)


def require_source_columns(integrated: DataFrame) -> None:
    """Fail early when the integrated-table contract is incomplete."""
    missing = sorted(REQUIRED_SOURCE_COLUMNS - set(integrated.columns))
    if missing:
        raise ValueError(
            "Integrated training source is missing required columns: "
            f"{missing}"
        )


def timestamp_literal(df: DataFrame, column: str, value: str) -> Column:
    """Cast a configured wall-clock timestamp to the source column's type."""
    data_type = df.schema[column].dataType.simpleString()
    return F.lit(value).cast(data_type)


def filter_observation_period(
    integrated: DataFrame,
    machine_learning: dict,
) -> DataFrame:
    """Keep the configured half-open observation window."""
    start = timestamp_literal(
        integrated,
        "pickup_hour",
        machine_learning["observation_start"],
    )
    end = timestamp_literal(
        integrated,
        "pickup_hour",
        machine_learning["observation_end_exclusive"],
    )
    return integrated.filter(
        (F.col("pickup_hour") >= start)
        & (F.col("pickup_hour") < end)
    )


def hourly_context(integrated: DataFrame) -> DataFrame:
    """Create one deterministic weather and air-quality record per hour."""
    aggregations = [
        F.max(F.col(column)).alias(column)
        for column in NUMERIC_CONTEXT_COLUMNS
    ]
    aggregations.extend(
        F.max(F.col(column).cast("int")).cast("boolean").alias(column)
        for column in AVAILABILITY_COLUMNS
    )
    return integrated.groupBy("pickup_hour").agg(*aggregations)


def pickup_zone_dimension(integrated: DataFrame) -> DataFrame:
    """Create one deterministic record for every observed valid pickup zone."""
    return (
        integrated
        .filter(F.col("pickup_location_id").isNotNull())
        .groupBy("pickup_location_id")
        .agg(
            F.max("pickup_zone").alias("pickup_zone"),
            F.max("pickup_borough").alias("pickup_borough"),
            F.max("pickup_service_zone").alias("pickup_service_zone"),
        )
    )


def assign_time_split(
    dataset: DataFrame,
    machine_learning: dict,
) -> DataFrame:
    """Assign deterministic chronological train, validation and test splits."""
    train_end = timestamp_literal(
        dataset,
        "pickup_hour",
        machine_learning["train_end_exclusive"],
    )
    validation_end = timestamp_literal(
        dataset,
        "pickup_hour",
        machine_learning["validation_end_exclusive"],
    )
    return dataset.withColumn(
        "split",
        F.when(F.col("pickup_hour") < train_end, F.lit("train"))
        .when(F.col("pickup_hour") < validation_end, F.lit("validation"))
        .otherwise(F.lit("test")),
    )


def hourly_taxi_demand_dataset(
    integrated: DataFrame,
    machine_learning: dict,
) -> DataFrame:
    """Transform trip-grain integrated data to zone-hour demand records.

    The full cross product of observed hours and pickup zones preserves valid
    zero-demand examples instead of silently dropping them during aggregation.
    Weather and air-quality values are hour-level context, while taxi-zone
    attributes are static location context.
    """
    require_source_columns(integrated)
    observations = filter_observation_period(integrated, machine_learning)

    hours = hourly_context(observations)
    zones = pickup_zone_dimension(observations)
    demand = (
        observations
        .groupBy("pickup_hour", "pickup_location_id")
        .agg(F.count(F.lit(1)).cast("long").alias("demand"))
    )

    complete_grid = hours.crossJoin(zones)
    dataset = (
        complete_grid
        .join(
            demand,
            on=["pickup_hour", "pickup_location_id"],
            how="left",
        )
        .fillna({"demand": 0})
    )
    return assign_time_split(dataset, machine_learning).select(*OUTPUT_COLUMNS)


def build_training_dataset(
    spark: SparkSession,
    config: dict,
) -> DataFrame:
    """Read the integrated Delta table and materialize the Task 1 dataset."""
    machine_learning = config["machine_learning"]
    source_path = table_path(config, machine_learning["source_table"])
    output_path = table_path(
        config,
        machine_learning["training_dataset_table"],
    )
    integrated = read_delta(spark, source_path)
    dataset = hourly_taxi_demand_dataset(integrated, machine_learning)
    write_delta(
        dataset,
        output_path,
        partitions=machine_learning.get("output_partitions"),
    )
    return read_delta(spark, output_path)


def print_dataset_summary(dataset: DataFrame) -> None:
    """Print compact evidence for the generated chronological splits."""
    summary = (
        dataset
        .groupBy("split")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum("demand").alias("trips"),
            F.sum((F.col("demand") == 0).cast("long")).alias("zero_demand_rows"),
            F.min("pickup_hour").alias("first_hour"),
            F.max("pickup_hour").alias("last_hour"),
        )
        .orderBy(
            F.when(F.col("split") == "train", 1)
            .when(F.col("split") == "validation", 2)
            .otherwise(3)
        )
    )
    summary.show(truncate=False)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/config.yaml"),
    )
    args = parser.parse_args()

    config = load_config(args.config)
    spark = create_spark()
    try:
        dataset = build_training_dataset(spark, config)
        output = table_path(
            config,
            config["machine_learning"]["training_dataset_table"],
        )
        print(f"Created hourly taxi-demand training dataset: {output}")
        print_dataset_summary(dataset)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
