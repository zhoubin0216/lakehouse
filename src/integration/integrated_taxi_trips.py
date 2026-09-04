from pyspark.sql import DataFrame


def build_integrated_taxi_trips(
    taxi_trips: DataFrame,
    pickup_zones: DataFrame,
    dropoff_zones: DataFrame,
    weather_hourly: DataFrame,
    air_quality_hourly: DataFrame,
) -> DataFrame:
    """Build the trip-level enriched analytical table.

    Join details will be implemented after silver schemas are finalized.
    """
    return taxi_trips
