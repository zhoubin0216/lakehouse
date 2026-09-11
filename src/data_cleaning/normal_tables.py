from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import read_delta, table_path, write_delta


def validate_primary_key(
    df: DataFrame,
    key_columns: list[str],
    dataset_name: str,
) -> None:
    """Verify that primary-key columns are present, non-null, and unique."""
    missing_columns = [
        column for column in key_columns if column not in df.columns
    ]
    if missing_columns:
        raise ValueError(
            f"{dataset_name}: missing primary-key columns: {missing_columns}"
        )

    null_condition = F.lit(False)
    for column in key_columns:
        null_condition = null_condition | F.col(column).isNull()

    null_key_count = df.filter(null_condition).count()
    if null_key_count:
        raise ValueError(
            f"{dataset_name}: found {null_key_count} rows with null primary keys"
        )

    duplicate_count = (
        df.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    if duplicate_count:
        raise ValueError(
            f"{dataset_name}: found {duplicate_count} duplicated primary keys"
        )


def clean_taxi_zones(df: DataFrame) -> DataFrame:
    """Create the standardized taxi-zone dimension table."""
    selected = df.select(
        "location_id",
        "borough",
        "zone",
        "service_zone",
    )

    validate_primary_key(
        selected,
        key_columns=["location_id"],
        dataset_name="taxi_zone_lookup",
    )

    return (
        selected
        .withColumn("borough", F.trim(F.col("borough")))
        .withColumn("zone", F.trim(F.col("zone")))
        .withColumn("service_zone", F.trim(F.col("service_zone")))
    )


def valid_or_null(
    column: F.Column,
    condition: F.Column,
) -> F.Column:
    """Keep valid measurements and replace invalid values with null."""
    return F.when(condition, column)


def clean_weather(df: DataFrame) -> DataFrame:
    """Create one standardized weather observation per hour."""
    selected = df.select(
        "year",
        "month",
        "day",
        "hour",
        F.col("temp").cast("double").alias("temperature_c"),
        F.col("rhum").cast("double").alias("relative_humidity_pct"),
        F.col("prcp").cast("double").alias("precipitation_mm"),
        F.col("snwd").cast("double").alias("snow_depth_mm"),
        F.col("wdir").cast("double").alias("wind_direction_deg"),
        F.col("wspd").cast("double").alias("wind_speed_kmh"),
        F.col("wpgt").cast("double").alias("wind_gust_kmh"),
        F.col("pres").cast("double").alias("pressure_hpa"),
        F.col("cldc").cast("double").alias("cloud_cover_pct"),
        F.col("coco").cast("integer").alias("weather_condition_code"),
    )

    validate_primary_key(
        selected,
        key_columns=["year", "month", "day", "hour"],
        dataset_name="weather_hourly",
    )

    cleaned = (
        selected
        .withColumn(
            "event_timestamp",
            F.make_timestamp_ntz(
                F.col("year"),
                F.col("month"),
                F.col("day"),
                F.col("hour"),
                F.lit(0),
                F.lit(0),
            ),
        )
        .withColumn(
            "relative_humidity_pct",
            valid_or_null(
                F.col("relative_humidity_pct"),
                F.col("relative_humidity_pct").between(0, 100),
            ),
        )
        .withColumn(
            "precipitation_mm",
            valid_or_null(
                F.col("precipitation_mm"),
                F.col("precipitation_mm") >= 0,
            ),
        )
        .withColumn(
            "snow_depth_mm",
            valid_or_null(
                F.col("snow_depth_mm"),
                F.col("snow_depth_mm") >= 0,
            ),
        )
        .withColumn(
            "wind_speed_kmh",
            valid_or_null(
                F.col("wind_speed_kmh"),
                F.col("wind_speed_kmh") >= 0,
            ),
        )
        .withColumn(
            "wind_gust_kmh",
            valid_or_null(
                F.col("wind_gust_kmh"),
                F.col("wind_gust_kmh") >= 0,
            ),
        )
    )

    validate_primary_key(
        cleaned,
        key_columns=["event_timestamp"],
        dataset_name="normal_weather_hourly",
    )

    return cleaned.select(
        "event_timestamp",
        F.col("event_timestamp").alias("event_hour"),
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
    )


def clean_air_quality(df: DataFrame) -> DataFrame:
    """Create one city-level PM2.5 observation per hour."""
    nyc_counties = ["Bronx", "Kings", "Queens"]
    expected_unit = "Micrograms/cubic meter (LC)"

    selected = (
        df.filter(
            (F.col("state_name") == "New York")
            & F.col("county_name").isin(*nyc_counties)
        )
        .select(
            "county_code",
            "site_num",
            "poc",
            "date_local",
            "time_local",
            F.col("sample_measurement")
            .cast("double")
            .alias("pm25_ug_m3"),
            "units_of_measure",
        )
        .withColumn(
            "event_timestamp",
            F.make_timestamp_ntz(
                F.year("date_local"),
                F.month("date_local"),
                F.dayofmonth("date_local"),
                F.hour("time_local"),
                F.minute("time_local"),
                F.second("time_local"),
            ),
        )
    )

    valid = selected.filter(
        F.col("event_timestamp").isNotNull()
        & F.col("pm25_ug_m3").isNotNull()
        & (F.col("pm25_ug_m3") >= 0)
        & (F.col("units_of_measure") == expected_unit)
    )

    hourly = (
        valid.groupBy(
            F.col("event_timestamp").alias("event_hour")
        )
        .agg(
            F.avg("pm25_ug_m3").alias("pm25_avg_ug_m3"),
            F.min("pm25_ug_m3").alias("pm25_min_ug_m3"),
            F.max("pm25_ug_m3").alias("pm25_max_ug_m3"),
            F.count("*").alias("air_quality_observation_count"),
            F.countDistinct(
                "county_code",
                "site_num",
            ).alias("air_quality_site_count"),
        )
        .withColumn(
            "event_timestamp",
            F.col("event_hour"),
        )
        .withColumn(
            "event_year",
            F.year("event_hour"),
        )
        .withColumn(
            "event_month",
            F.month("event_hour"),
        )
    )

    validate_primary_key(
        hourly,
        key_columns=["event_hour"],
        dataset_name="normal_air_quality_hourly",
    )

    return hourly.select(
        "event_timestamp",
        "event_hour",
        "pm25_avg_ug_m3",
        "pm25_min_ug_m3",
        "pm25_max_ug_m3",
        "air_quality_observation_count",
        "air_quality_site_count",
        "event_year",
        "event_month",
    )


def clean_taxi_trips(
    df: DataFrame,
    dataset_config: dict,
) -> DataFrame:
    """Clean taxi trips and derive fields needed for integration."""
    rules = dataset_config["quality_rules"]
    pickup_start = rules["pickup_start"]
    pickup_end = rules["pickup_end_exclusive"]
    max_duration = rules["max_trip_duration_seconds"]
    max_distance = rules["max_trip_distance"]
    duration_seconds = F.expr(
        "timestampdiff(SECOND, pickup_timestamp, dropoff_timestamp)"
    )

    selected = df.select(
        F.col("_record_hash").alias("trip_id"),
        "vendor_id",
        "pickup_timestamp",
        "dropoff_timestamp",
        "passenger_count",
        "trip_distance",
        "ratecode_id",
        "store_and_fwd_flag",
        "pickup_location_id",
        "dropoff_location_id",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
    )

    valid_trips = (
        selected
        .filter(
            (F.col("pickup_timestamp") >=
             F.lit(pickup_start).cast("timestamp_ntz"))
            & (F.col("pickup_timestamp") <
               F.lit(pickup_end).cast("timestamp_ntz"))
        )
        .filter(F.col("trip_id").isNotNull())
        .filter(F.col("pickup_timestamp").isNotNull())
        .filter(F.col("dropoff_timestamp").isNotNull())
        .filter(F.col("pickup_location_id").isNotNull())
        .filter(F.col("dropoff_location_id").isNotNull())
        .withColumn("trip_duration_seconds", duration_seconds)
        .filter(
            (F.col("trip_duration_seconds") > 0)
            & (F.col("trip_duration_seconds") <= max_duration)
        )
        .dropDuplicates(["trip_id"])
    )

    return (
        valid_trips
        .withColumn(
            "has_invalid_distance",
            F.col("trip_distance").isNull()
            | (F.col("trip_distance") < 0)
            | (F.col("trip_distance") > max_distance),
        )
        .withColumn(
            "is_zero_distance",
            F.col("trip_distance") == 0,
        )
        .withColumn(
            "trip_distance",
            valid_or_null(
                F.col("trip_distance"),
                F.col("trip_distance").between(0, max_distance),
            ),
        )
        .withColumn(
            "is_financial_adjustment",
            (F.col("fare_amount") < 0)
            | (F.col("total_amount") < 0),
        )
        .withColumn(
            "trip_duration_minutes",
            F.col("trip_duration_seconds") / F.lit(60.0),
        )
        .withColumn(
            "pickup_hour",
            F.date_trunc(
                "hour",
                F.col("pickup_timestamp"),
            ).cast("timestamp_ntz"),
        )
        .withColumn(
            "pickup_date",
            F.to_date("pickup_timestamp"),
        )
        .withColumn(
            "pickup_year",
            F.year("pickup_timestamp"),
        )
        .withColumn(
            "pickup_month",
            F.month("pickup_timestamp"),
        )
    )


def build_normal_tables(spark: SparkSession, config: dict) -> None:
    """Validate, clean, standardize, and write normal Delta tables."""
    zone_config = config["datasets"]["taxi_zone_lookup"]
    zone_raw = read_delta(
        spark,
        table_path(config, zone_config["raw_table"]),
    )
    zone_normal = clean_taxi_zones(zone_raw)
    write_delta(
        zone_normal,
        table_path(config, zone_config["normal_table"]),
    )

    weather_config = config["datasets"]["weather_hourly"]
    weather_raw = read_delta(
        spark,
        table_path(config, weather_config["raw_table"]),
    )
    weather_normal = clean_weather(weather_raw)
    write_delta(
        weather_normal,
        table_path(config, weather_config["normal_table"]),
    )
    air_quality_config = config["datasets"]["air_quality"]
    air_quality_raw = read_delta(
        spark,
        table_path(config, air_quality_config["raw_table"]),
    )
    air_quality_normal = clean_air_quality(air_quality_raw)
    write_delta(
        air_quality_normal,
        table_path(config, air_quality_config["normal_table"]),
        partitions=air_quality_config.get("partitions"),
    )

    taxi_config = config["datasets"]["yellow_taxi_trips"]
    taxi_raw = read_delta(
        spark,
        table_path(config, taxi_config["raw_table"]),
    )
    taxi_normal = clean_taxi_trips(
        taxi_raw,
        taxi_config,
    )
    write_delta(
        taxi_normal,
        table_path(config, taxi_config["normal_table"]),
        partitions=taxi_config.get("partitions"),
    )
