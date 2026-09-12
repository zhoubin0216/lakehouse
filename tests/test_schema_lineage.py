from datetime import date, datetime

from pyspark.sql import SparkSession

from src.data_aggregation.aggregate_tables import trips_per_borough
from src.data_analysis.benchmark import prepare_benchmark_data
from src.schema_lineage import collect_schema_version_snapshot


def test_aggregate_and_benchmark_preserve_schema_version_sets(
    spark: SparkSession,
) -> None:
    columns = [
        "trip_id",
        "vendor_id",
        "pickup_timestamp",
        "dropoff_timestamp",
        "passenger_count",
        "trip_distance",
        "pickup_location_id",
        "dropoff_location_id",
        "payment_type",
        "fare_amount",
        "total_amount",
        "trip_duration_seconds",
        "pickup_date",
        "pickup_year",
        "pickup_month",
        "pickup_borough",
        "taxi_schema_version",
        "pickup_zone_schema_version",
        "dropoff_zone_schema_version",
        "weather_schema_version",
        "air_quality_schema_versions",
    ]
    rows = [
        (
            "trip-a", 1, datetime(2024, 1, 2, 12), datetime(2024, 1, 2, 12, 10),
            1, 2.0, 1, 2, 1, 10.0, 12.0, 600, date(2024, 1, 2), 2024, 1,
            "Manhattan", 1, 1, 1, 1, [1],
        ),
        (
            "trip-b", 1, datetime(2024, 1, 2, 13), datetime(2024, 1, 2, 13, 10),
            1, 2.0, 1, 2, 1, 11.0, 13.0, 600, date(2024, 1, 2), 2024, 1,
            "Manhattan", 2, 2, 1, 2, [1, 2],
        ),
    ]
    integrated = spark.createDataFrame(rows, columns)

    benchmark = prepare_benchmark_data(integrated)
    aggregate = trips_per_borough(integrated).first()
    snapshot = collect_schema_version_snapshot(benchmark)

    assert aggregate.trip_count == 2
    assert aggregate.taxi_schema_versions == [1, 2]
    assert aggregate.pickup_zone_schema_versions == [1, 2]
    assert aggregate.dropoff_zone_schema_versions == [1]
    assert aggregate.weather_schema_versions == [1, 2]
    assert aggregate.air_quality_schema_versions == [1, 2]
    assert snapshot == {
        "taxi_schema_versions": [1, 2],
        "pickup_zone_schema_versions": [1, 2],
        "dropoff_zone_schema_versions": [1],
        "weather_schema_versions": [1, 2],
        "air_quality_schema_versions": [1, 2],
    }
