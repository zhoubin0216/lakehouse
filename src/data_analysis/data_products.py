"""Build the reusable analytical Delta products required by Week 2 Task 4."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import time

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common import create_spark, load_config, table_path, write_delta
from src.data_analysis.product_metadata import (
    attach_product_metadata,
    delta_snapshot,
    load_catalog_records,
    write_product_catalog,
)
from src.schema_lineage import collect_schema_version_snapshot, schema_version_aggregations
from src.data_analysis.weather_conditions import weather_condition_name_column


def daily_mobility_summary(integrated: DataFrame) -> DataFrame:
    """Produce one operational mobility summary per pickup date."""
    daily = integrated.groupBy("pickup_date", "pickup_year", "pickup_month").agg(
        F.count("*").alias("trip_count"),
        F.sum("passenger_count").alias("passenger_count"),
        F.avg("trip_distance").alias("avg_trip_distance"),
        F.avg("trip_duration_seconds").alias("avg_trip_duration_seconds"),
        F.avg("fare_amount").alias("avg_fare_amount"),
        F.sum("total_amount").alias("total_revenue"),
        *schema_version_aggregations(integrated),
    )
    hourly = (
        integrated.groupBy(
            "pickup_date",
            F.hour("pickup_timestamp").alias("hour_of_day"),
        )
        .agg(F.count("*").alias("hourly_trip_count"))
        .withColumn(
            "rank",
            F.row_number().over(
                Window.partitionBy("pickup_date").orderBy(
                    F.desc("hourly_trip_count"),
                    F.asc("hour_of_day"),
                )
            ),
        )
        .filter(F.col("rank") == 1)
        .select(
            "pickup_date",
            F.col("hour_of_day").alias("peak_hour"),
            F.col("hourly_trip_count").alias("peak_hour_trip_count"),
        )
    )
    return daily.join(hourly, "pickup_date", "left").orderBy("pickup_date")


def taxi_zone_statistics(integrated: DataFrame) -> DataFrame:
    """Produce monthly demand and trip metrics for every pickup taxi zone."""
    return (
        integrated.filter(
            F.col("pickup_location_id").isNotNull()
            & F.col("pickup_zone").isNotNull()
        )
        .groupBy(
            "pickup_year",
            "pickup_month",
            "pickup_location_id",
            "pickup_zone",
            "pickup_borough",
        )
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg("trip_duration_seconds").alias("avg_trip_duration_seconds"),
            F.avg("fare_amount").alias("avg_fare_amount"),
            F.sum("total_amount").alias("total_revenue"),
            *schema_version_aggregations(integrated),
        )
        .orderBy("pickup_year", "pickup_month", "pickup_location_id")
    )


def weather_impact_summary(integrated: DataFrame) -> DataFrame:
    """Produce monthly mobility metrics for each observed weather condition."""
    labeled = integrated.withColumn(
        "weather_condition",
        weather_condition_name_column(),
    )
    return (
        labeled.filter(F.col("weather_condition_code").isNotNull())
        .groupBy(
            "pickup_year",
            "pickup_month",
            "weather_condition_code",
            "weather_condition",
        )
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg("trip_duration_seconds").alias("avg_trip_duration_seconds"),
            F.avg("fare_amount").alias("avg_fare_amount"),
            *schema_version_aggregations(labeled),
        )
        .orderBy("pickup_year", "pickup_month", "weather_condition_code")
    )


def pm25_category(column: str = "pm25_avg_ug_m3"):
    """Map PM2.5 values into stable project-defined analytical bands."""
    value = F.col(column)
    return (
        F.when(value <= 9.0, "good")
        .when(value <= 35.4, "moderate")
        .when(value <= 55.4, "unhealthy_sensitive")
        .when(value <= 125.4, "unhealthy")
        .when(value <= 225.4, "very_unhealthy")
        .otherwise("hazardous")
    )


def air_quality_impact_summary(integrated: DataFrame) -> DataFrame:
    """Produce monthly mobility metrics by PM2.5 concentration category."""
    categorized = (
        integrated.filter(F.col("pm25_avg_ug_m3").isNotNull())
        .withColumn("air_quality_category", pm25_category())
    )
    return (
        categorized.groupBy("pickup_year", "pickup_month", "air_quality_category")
        .agg(
            F.avg("pm25_avg_ug_m3").alias("avg_pm25_ug_m3"),
            F.count("*").alias("trip_count"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg("trip_duration_seconds").alias("avg_trip_duration_seconds"),
            *schema_version_aggregations(categorized),
        )
        .orderBy("pickup_year", "pickup_month", "air_quality_category")
    )


PRODUCT_BUILDERS = {
    "daily_mobility_summary": daily_mobility_summary,
    "taxi_zone_statistics": taxi_zone_statistics,
    "weather_impact_summary": weather_impact_summary,
    "air_quality_impact_summary": air_quality_impact_summary,
}


def build_data_products(
    spark: SparkSession,
    config: dict,
    product_name: str | None = None,
    scope_predicate: str | None = None,
    source_version: int | None = None,
) -> list[dict]:
    """Refresh one or all products from one pinned Integrated Delta snapshot."""
    definitions = config["data_analysis"]["products"]["definitions"]
    names = [product_name] if product_name else list(PRODUCT_BUILDERS)
    unknown = [
        name
        for name in names
        if name not in PRODUCT_BUILDERS or name not in definitions
    ]
    if unknown:
        raise ValueError(f"Unknown analytical products: {unknown}")

    source_table = config["data_integration"]["integrated_taxi_trips_table"]
    source_path = table_path(config, source_table)
    source_state = delta_snapshot(spark, source_path)
    if source_version is not None:
        source_state["version"] = source_version
    integrated = (
        spark.read.format("delta")
        .option("versionAsOf", source_state["version"])
        .load(source_path)
    )
    existing = load_catalog_records(spark, config)
    refreshed_records = []

    for name in names:
        definition = definitions[name]
        refreshed_at = datetime.now(timezone.utc).replace(tzinfo=None)
        created_at = existing.get(name, {}).get("created_at") or refreshed_at
        started = time.perf_counter()
        output_path = table_path(config, definition["table"])
        from delta.tables import DeltaTable
        from src.incremental.storage import replace_scope
        predicate = scope_predicate if DeltaTable.isDeltaTable(spark, output_path) else None
        source = integrated.filter(predicate) if predicate else integrated
        # Row metadata describes the source scope actually recomputed.
        source_versions = collect_schema_version_snapshot(source)
        product = PRODUCT_BUILDERS[name](source)
        product = attach_product_metadata(
            product,
            product_name=name,
            product_schema_version=definition["schema_version"],
            source_table=source_table,
            source_delta_version=source_state["version"],
            source_schema_versions=source_versions,
            created_at=created_at,
            refreshed_at=refreshed_at,
        ).persist(StorageLevel.MEMORY_AND_DISK)
        try:
            row_count = product.count()
            if predicate:
                replace_scope(product, output_path, predicate)
                row_count = spark.read.format("delta").load(output_path).count()
            else:
                write_delta(product, output_path, partitions=definition.get("partitions"))
        finally:
            product.unpersist(blocking=True)
        output_state = delta_snapshot(spark, output_path)
        duration = time.perf_counter() - started
        record = {
            "product_name": name,
            "table_path": definition["table"],
            "description": definition["description"],
            "intended_users": definition["intended_users"],
            "grain": definition["grain"],
            "materialization_reason": definition["materialization_reason"],
            "source_table": source_table,
            "source_delta_version": source_state["version"],
            "source_schema_versions": json.dumps(source_versions, sort_keys=True),
            "product_schema_version": definition["schema_version"],
            "created_at": created_at,
            "refreshed_at": refreshed_at,
            "row_count": row_count,
            "storage_bytes": output_state["size_bytes"],
            "refresh_duration_seconds": duration,
            "partitions": definition.get("partitions", []),
        }
        refreshed_records.append(record)
        print(
            f"Created analytical product {name}: {row_count} row(s), "
            f"Delta version {output_state['version']} at {output_path}"
        )

    write_product_catalog(spark, config, refreshed_records, existing)
    return refreshed_records


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/config.yaml"))
    parser.add_argument("--product", choices=list(PRODUCT_BUILDERS))
    parser.add_argument("--list", action="store_true", help="List products without starting Spark")
    args = parser.parse_args()
    if args.list:
        print("\n".join(PRODUCT_BUILDERS))
        return
    config = load_config(args.config)
    spark = create_spark()
    try:
        build_data_products(spark, config, args.product)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
