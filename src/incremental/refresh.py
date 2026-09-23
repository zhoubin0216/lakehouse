"""Recompute complete affected groups; never average old and new averages."""
from delta.tables import DeltaTable

from src.common import table_path
from src.data_aggregation.aggregate_tables import (
    trips_per_borough, avg_trip_duration_per_day, avg_fare_per_borough,
)
from src.incremental.storage import changed_rows, read, replace_scope, values_predicate


LINEAGE = ["taxi_schema_version", "pickup_zone_schema_version", "dropoff_zone_schema_version",
           "weather_schema_version", "air_quality_schema_versions"]
PRODUCT_INPUTS = {
    "daily_mobility_summary": ["pickup_timestamp", "pickup_date", "pickup_year", "pickup_month",
                               "passenger_count", "trip_distance", "trip_duration_seconds", "fare_amount", "total_amount"],
    "taxi_zone_statistics": ["pickup_year", "pickup_month", "pickup_location_id", "pickup_zone", "pickup_borough",
                            "trip_distance", "trip_duration_seconds", "fare_amount", "total_amount"],
    "weather_impact_summary": ["pickup_year", "pickup_month", "weather_condition_code",
                              "trip_distance", "trip_duration_seconds", "fare_amount"],
    "air_quality_impact_summary": ["pickup_year", "pickup_month", "pm25_avg_ug_m3", "trip_distance", "trip_duration_seconds"],
}


def affected_product_rows(before, after, name):
    columns = ["trip_id", *PRODUCT_INPUTS[name], *LINEAGE]
    ids = changed_rows(after.select(*columns), before.select(*columns), ["trip_id"]).select("trip_id")
    return after.unionByName(before, allowMissingColumns=True).join(ids, "trip_id", "left_semi")


def refresh_aggregates(spark, config, changes):
    integrated = read(spark, table_path(config, config["data_integration"]["integrated_taxi_trips_table"]))
    definitions = [
        ("trips_per_borough_table", trips_per_borough, "pickup_borough"),
        ("avg_fare_per_borough_table", avg_fare_per_borough, "pickup_borough"),
        ("avg_trip_duration_per_day_table", avg_trip_duration_per_day, "pickup_date"),
    ]
    for key, builder, group in definitions:
        path = table_path(config, config["data_aggregation"][key])
        values = [row[0] for row in changes.select(group).distinct().collect()]
        predicate = values_predicate(group, values) if DeltaTable.isDeltaTable(spark, path) else "true"
        replace_scope(builder(integrated.filter(predicate)), path, predicate)
