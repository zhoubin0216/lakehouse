from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import read_delta, table_path, write_delta


def trips_per_borough(df: DataFrame) -> DataFrame:
    """Count taxi trips by pickup borough."""
    return (
        df.groupBy("pickup_borough")
        .agg(
            F.count("*").alias("trip_count"),
        )
        .orderBy(F.desc("trip_count"))
    )


def avg_trip_duration_per_day(df: DataFrame) -> DataFrame:
    """Calculate average trip duration for each pickup date."""
    return (
        df.groupBy("pickup_date")
        .agg(
            F.avg("trip_duration_seconds")
            .alias("avg_trip_duration_seconds"),
        )
        .orderBy("pickup_date")
    )


def avg_fare_per_borough(df: DataFrame) -> DataFrame:
    """Calculate average fare amount by pickup borough."""
    return (
        df.groupBy("pickup_borough")
        .agg(
            F.avg("fare_amount").alias("avg_fare_amount"),
        )
        .orderBy("pickup_borough")
    )


def build_aggregate_tables(
    spark: SparkSession,
    config: dict,
) -> None:
    """Create aggregate tables used by reports and analysis."""

    integration_config = config["data_integration"]

    integrated = read_delta(
        spark,
        table_path(
            config,
            integration_config["integrated_taxi_trips_table"],
        ),
    )

    aggregation_config = config["data_aggregation"]

    tables = {
        "trips_per_borough_table":
            trips_per_borough(integrated),

        "avg_trip_duration_per_day_table":
            avg_trip_duration_per_day(integrated),

        "avg_fare_per_borough_table":
            avg_fare_per_borough(integrated),
    }

    for config_key, dataframe in tables.items():
        output_path = table_path(
            config,
            aggregation_config[config_key],
        )

        write_delta(
            dataframe,
            output_path,
        )

        print(f"Created aggregate table: {output_path}")