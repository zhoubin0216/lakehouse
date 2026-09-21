from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import read_delta, table_path, write_delta
from src.data_cleaning.normal_tables import optional_double


def prepare_pickup_zones(zones: DataFrame) -> DataFrame:
    """Rename taxi-zone fields for the pickup-side join."""
    return F.broadcast(
        zones.select(
            F.col("location_id").alias("pickup_location_id"),
            F.col("zone").alias("pickup_zone"),
            F.col("borough").alias("pickup_borough"),
            F.col("service_zone").alias("pickup_service_zone"),
            F.col("source_schema_version").alias("pickup_zone_schema_version"),
        )
    )


def prepare_dropoff_zones(zones: DataFrame) -> DataFrame:
    """Rename taxi-zone fields for the dropoff-side join."""
    return F.broadcast(
        zones.select(
            F.col("location_id").alias("dropoff_location_id"),
            F.col("zone").alias("dropoff_zone"),
            F.col("borough").alias("dropoff_borough"),
            F.col("service_zone").alias("dropoff_service_zone"),
            F.col("source_schema_version").alias("dropoff_zone_schema_version"),
        )
    )


def prepare_weather(weather: DataFrame) -> DataFrame:
    """Select weather fields needed by the integrated table."""
    return F.broadcast(
        weather.select(
            F.col("event_hour").alias("pickup_hour"),
            "temperature_c",
            "relative_humidity_pct",
            optional_double(weather, "humidity").alias("humidity"),
            "precipitation_mm",
            "snow_depth_mm",
            "wind_direction_deg",
            "wind_speed_kmh",
            "wind_gust_kmh",
            "pressure_hpa",
            "cloud_cover_pct",
            "weather_condition_code",
            F.col("source_schema_version").alias("weather_schema_version"),
        )
    )


def prepare_air_quality(air_quality: DataFrame) -> DataFrame:
    """Select air-quality fields needed by the integrated table."""
    return F.broadcast(
        air_quality.select(
            F.col("event_hour").alias("pickup_hour"),
            "pm25_avg_ug_m3",
            "pm25_min_ug_m3",
            "pm25_max_ug_m3",
            optional_double(air_quality, "aqi").alias("aqi"),
            "air_quality_observation_count",
            "air_quality_site_count",
            F.col("source_schema_versions").alias("air_quality_schema_versions"),
        )
    )


def integrate_taxi_trips(
    taxi_trips: DataFrame,
    zones: DataFrame,
    weather: DataFrame,
    air_quality: DataFrame,
) -> DataFrame:
    """Enrich each taxi trip without removing unmatched trips."""
    integrated = (
        taxi_trips.withColumnRenamed(
            "source_schema_version",
            "taxi_schema_version",
        )
        .join(
            prepare_pickup_zones(zones),
            on="pickup_location_id",
            how="left",
        )
        .join(
            prepare_dropoff_zones(zones),
            on="dropoff_location_id",
            how="left",
        )
        .join(
            prepare_weather(weather),
            on="pickup_hour",
            how="left",
        )
        .join(
            prepare_air_quality(air_quality),
            on="pickup_hour",
            how="left",
        )
        .withColumn(
            "weather_available",
            F.col("temperature_c").isNotNull(),
        )
        .withColumn(
            "air_quality_available",
            F.col("pm25_avg_ug_m3").isNotNull(),
        )
    )

    return integrated


def build_integrated_tables(
    spark: SparkSession,
    config: dict,
) -> None:
    """Build the analysis-ready integrated taxi-trip table."""
    datasets = config["datasets"]

    taxi_trips = read_delta(
        spark,
        table_path(
            config,
            datasets["yellow_taxi_trips"]["normal_table"],
        ),
    )
    zones = read_delta(
        spark,
        table_path(
            config,
            datasets["taxi_zone_lookup"]["normal_table"],
        ),
    )
    weather = read_delta(
        spark,
        table_path(
            config,
            datasets["weather_hourly"]["normal_table"],
        ),
    )
    air_quality = read_delta(
        spark,
        table_path(
            config,
            datasets["air_quality"]["normal_table"],
        ),
    )

    integrated = integrate_taxi_trips(
        taxi_trips,
        zones,
        weather,
        air_quality,
    )

    integration_config = config["data_integration"]
    write_delta(
        integrated,
        table_path(
            config,
            integration_config["integrated_taxi_trips_table"],
        ),
        partitions=integration_config.get("partitions"),
    )
