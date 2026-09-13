from datetime import datetime, date
from pathlib import Path

import pytest

from src.data_analysis.analytical_queries import ANALYTICAL_QUERIES


@pytest.fixture
def analytical_views(spark):
    # Jan 1: two trips in sun, no trips in rain, one trip later in sun.
    trips = [
        (1, "A", "X", datetime(2024, 1, 1, 10), date(2024, 1, 1), 2024, 1, 1, 2.0),
        (1, "A", "X", datetime(2024, 1, 1, 10), date(2024, 1, 1), 2024, 1, 1, 4.0),
        (2, "B", "X", datetime(2024, 1, 1, 12), date(2024, 1, 1), 2024, 1, 1, 6.0),
    ]
    schema = ("pickup_location_id int, pickup_zone string, pickup_borough string, "
              "pickup_timestamp timestamp_ntz, pickup_date date, pickup_year int, "
              "pickup_month int, weather_condition_code int, trip_distance double")
    df = spark.createDataFrame(trips, schema)
    from pyspark.sql import functions as F
    df.withColumn("pickup_hour", F.col("pickup_timestamp")) \
      .createOrReplaceTempView("integrated_taxi_trips")
    spark.createDataFrame([
        (datetime(2024, 1, 1, 10), 1),
        (datetime(2024, 1, 1, 11), 2),
        (datetime(2024, 1, 1, 12), 1),
    ], "event_hour timestamp_ntz, weather_condition_code int") \
      .createOrReplaceTempView("analysis_weather")
    spark.createDataFrame([
        (datetime(2024, 1, 1, 10), 10.0),
        (datetime(2024, 1, 1, 11), 20.0),
        (datetime(2024, 1, 1, 12), 30.0),
    ], "event_hour timestamp_ntz, pm25_avg_ug_m3 double") \
      .createOrReplaceTempView("analysis_air_quality")


def test_all_queries_execute_and_have_expected_results(spark, analytical_views):
    results = {name: spark.sql(sql).collect() for name, sql in ANALYTICAL_QUERIES.items()}
    assert sum(row.trip_count for row in results["monthly_zone_demand"]) == 3
    assert results["avg_distance_by_weather"][0].avg_trip_distance == 4.0
    air = results["air_quality_demand_relationship"][0]
    assert air.hours_analyzed == 3
    assert air.avg_hourly_trip_count == 1.0
    assert air.pm25_demand_correlation == -0.5
    variation = {r.pickup_zone: r for r in results["zone_weather_variation"]}
    assert variation["A"].demand_variation == 1.0
    assert variation["A"].demand_stddev == 0.5
    assert variation["B"].demand_variation == 0.5
    assert results["peak_hour_by_weekday"][0].peak_hour == 10
    assert results["monthly_demand_trend"][0].month_over_month_change is None


def test_peak_ties_and_missing_month(spark):
    spark.sql("""SELECT * FROM VALUES
        (2024, 1, DATE '2024-01-01', TIMESTAMP_NTZ '2024-01-01 10:00:00'),
        (2024, 3, DATE '2024-03-04', TIMESTAMP_NTZ '2024-03-04 11:00:00')
        AS t(pickup_year, pickup_month, pickup_date, pickup_timestamp)
    """).createOrReplaceTempView("integrated_taxi_trips")
    peaks = spark.sql(ANALYTICAL_QUERIES["peak_hour_by_weekday"]).collect()
    assert {row.peak_hour for row in peaks} == {10, 11}
    months = spark.sql(ANALYTICAL_QUERIES["monthly_demand_trend"]).collect()
    assert months[1].month_over_month_change is None


def test_month_change_across_year(spark):
    spark.sql("""SELECT * FROM VALUES (2023, 12), (2024, 1), (2024, 1)
        AS t(pickup_year, pickup_month)""").createOrReplaceTempView("integrated_taxi_trips")
    row = spark.sql(ANALYTICAL_QUERIES["monthly_demand_trend"]).collect()[1]
    assert row.month_over_month_change == 1
    assert row.month_over_month_change_pct == 100.0


def test_context_registration_limits_observation_period(spark, analytical_views, monkeypatch):
    import src.data_analysis.analytical_queries as queries
    taxi = spark.table("integrated_taxi_trips")
    weather = spark.table("analysis_weather")
    air = spark.table("analysis_air_quality")
    outside = spark.createDataFrame([(datetime(2024, 2, 1), 99)], weather.schema)
    sources = {"taxi": taxi, "weather": weather.union(outside), "air": air}
    monkeypatch.setattr(queries, "read_delta", lambda session, path: sources[Path(path).name])
    config = {"paths": {"lakehouse": "."},
              "data_integration": {"integrated_taxi_trips_table": "taxi"},
              "datasets": {"weather_hourly": {"normal_table": "weather"},
                           "air_quality": {"normal_table": "air"}}}
    queries.register_analysis_views(spark, config)
    assert spark.table("analysis_weather").count() == 3
    sources["taxi"] = taxi.limit(0)
    queries.register_analysis_views(spark, config)
    assert spark.table("analysis_weather").count() == 0
    assert spark.table("analysis_air_quality").count() == 0
