from __future__ import annotations

import argparse
from pathlib import Path

from src.common import create_spark, load_config, read_delta, table_path, write_delta

from src.data_analysis.query_library import ANALYTICAL_QUERIES, INTEGRATED_VIEW


def register_analysis_views(spark, config):
    """Use the observed taxi period, including zero-demand hours inside it.

    This assumes the taxi source is complete between its first and last hour.
    Unobserved context is excluded, never filled with a fabricated measurement.
    """
    integrated = read_delta(spark, table_path(
        config, config["data_integration"]["integrated_taxi_trips_table"]))
    integrated.createOrReplaceTempView(INTEGRATED_VIEW)
    bounds = spark.sql("SELECT MIN(pickup_hour) AS first_hour, MAX(pickup_hour) AS last_hour "
                       "FROM integrated_taxi_trips").first()
    from pyspark.sql import functions as F
    from src.data_cleaning.normal_tables import pickup_period_condition
    for dataset, view in (("weather_hourly", "analysis_weather"),
                          ("air_quality", "analysis_air_quality")):
        df = read_delta(spark, table_path(config, config["datasets"][dataset]["normal_table"]))
        rules = config.get("datasets", {}).get("yellow_taxi_trips", {}).get("quality_rules")
        if rules:
            df = df.filter(pickup_period_condition("event_hour", rules))
        (df.filter(F.col("event_hour").between(bounds.first_hour, bounds.last_hour))
           .createOrReplaceTempView(view))


def run_analytical_queries(spark, config, query_name=None, save=False):
    register_analysis_views(spark, config)
    names = [query_name] if query_name else list(ANALYTICAL_QUERIES)
    for name in names:
        result = spark.sql(ANALYTICAL_QUERIES[name])
        print(f"Query: {name}")
        result.show(50, truncate=False)
        if save:
            root = config.get("data_analysis", {}).get("query_results_root", "analysis/query_results")
            write_delta(result, table_path(config, f"{root}/{name}"))


def main():
    parser = argparse.ArgumentParser(description="Execute analytical Spark SQL queries")
    parser.add_argument("--query", choices=list(ANALYTICAL_QUERIES))
    parser.add_argument("--save", action="store_true", help="Also save results as Delta tables")
    parser.add_argument("--config", type=Path, default=Path("configs/config.yaml"))
    parser.add_argument("--list", action="store_true", help="List query names without starting Spark")
    args = parser.parse_args()
    if args.list:
        print("\n".join(ANALYTICAL_QUERIES))
        return
    config = load_config(args.config)
    spark = create_spark()
    try:
        run_analytical_queries(spark, config, args.query, args.save)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
