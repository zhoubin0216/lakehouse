from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common import read_delta, table_path, write_delta
from src.data_quality import (
    classify_records,
    combine_rejections,
    minimum_rule,
    non_empty_text_rule,
    predicate_rule,
    range_rule,
    required_rule,
    write_rejected_records,
)


def latest_schema_records(df: DataFrame, key_columns: list[str]) -> DataFrame:
    """Keep the latest arrival; schema version is a contract, not revision time."""
    lineage_columns = ["_schema_version", "_ingestion_timestamp"]
    missing_columns = [
        column
        for column in [*key_columns, *lineage_columns]
        if column not in df.columns
    ]
    if missing_columns:
        raise ValueError(f"Raw input is missing version lineage columns: {missing_columns}")

    window = Window.partitionBy(*key_columns).orderBy(
        F.col("_ingestion_timestamp").desc_nulls_last(),
        F.col("_schema_version").desc(),
    )
    return (
        df.withColumn("_schema_rank", F.row_number().over(window))
        .filter(F.col("_schema_rank") == 1)
        .drop("_schema_rank")
    )


def latest_complete_records(
    df: DataFrame,
    key_columns: list[str],
) -> tuple[DataFrame, DataFrame]:
    """Quarantine incomplete business keys before selecting the latest revision."""
    complete, incomplete = classify_records(
        df,
        stage="cleaning",
        rejection_rules=[required_rule(column) for column in key_columns],
    )
    return latest_schema_records(complete, key_columns), incomplete


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
    cleaned, _ = clean_taxi_zones_with_rejections(df)
    return cleaned


def clean_taxi_zones_with_rejections(
    df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Create the standardized taxi-zone dimension table."""
    latest, incomplete = latest_complete_records(df, ["location_id"])
    selected = latest.select(
        "location_id",
        "borough",
        "zone",
        "service_zone",
        F.col("_schema_version").alias("source_schema_version"),
    )

    accepted, rejected = classify_records(
        selected,
        stage="cleaning",
        rejection_rules=[
            non_empty_text_rule("borough"),
            non_empty_text_rule("zone"),
            non_empty_text_rule("service_zone"),
        ],
    )

    validate_primary_key(
        accepted,
        key_columns=["location_id"],
        dataset_name="taxi_zone_lookup",
    )

    cleaned = (
        accepted
        .withColumn("borough", F.trim(F.col("borough")))
        .withColumn("zone", F.trim(F.col("zone")))
        .withColumn("service_zone", F.trim(F.col("service_zone")))
    )
    return cleaned, combine_rejections(incomplete, rejected)


def missing_text(column: str):
    return F.col(column).isNull() | (F.trim(F.col(column)) == "")


def valid_or_null(
    column: F.Column,
    condition: F.Column,
) -> F.Column:
    """Keep valid measurements and replace invalid values with null."""
    return F.when(condition, column)


def clean_weather(df: DataFrame) -> DataFrame:
    cleaned, _ = clean_weather_with_rejections(df)
    return cleaned


def clean_weather_with_rejections(
    df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Create one standardized weather observation per hour."""
    latest, incomplete = latest_complete_records(df, ["year", "month", "day", "hour"])
    selected = latest.select(
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
        optional_double(df, "humidity").alias("humidity"),
        F.col("_schema_version").alias("source_schema_version"),
    )

    with_timestamp = selected.withColumn(
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
    accepted, rejected = classify_records(
        with_timestamp,
        stage="cleaning",
        rejection_rules=[
            predicate_rule(
                "weather.valid_timestamp",
                "invalid event timestamp",
                "invalid_value",
                lambda _df: F.col("event_timestamp").isNull(),
            ),
            range_rule(
                "relative_humidity_pct", 0, 100,
                "relative humidity must be between 0 and 100",
            ),
            minimum_rule("precipitation_mm", 0, "precipitation must be non-negative"),
            minimum_rule("snow_depth_mm", 0, "snow depth must be non-negative"),
            range_rule(
                "wind_direction_deg", 0, 360,
                "wind direction must be between 0 and 360",
            ),
            minimum_rule("wind_speed_kmh", 0, "wind speed must be non-negative"),
            minimum_rule("wind_gust_kmh", 0, "wind gust must be non-negative"),
            range_rule(
                "cloud_cover_pct", 0, 100,
                "cloud cover must be between 0 and 100",
            ),
            range_rule("humidity", 0, 100, "humidity must be between 0 and 100"),
        ],
    )

    cleaned = (
        accepted
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

    result = cleaned.select(
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
        "source_schema_version",
        "humidity",
    )
    return result, combine_rejections(incomplete, rejected)


def clean_air_quality(df: DataFrame) -> DataFrame:
    cleaned, _ = clean_air_quality_with_rejections(df)
    return cleaned


def clean_air_quality_with_rejections(
    df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Create one city-level PM2.5 observation per hour."""
    nyc_counties = ["Bronx", "Kings", "Queens"]
    expected_unit = "Micrograms/cubic meter (LC)"
    df = normalize_air_times(df)

    key_columns = [
        "state_code",
        "county_code",
        "site_num",
        "parameter_code",
        "poc",
        "date_local",
        "time_local",
    ]
    latest, incomplete = latest_complete_records(
        df.filter(
            (F.col("state_name") == "New York")
            & F.col("county_name").isin(*nyc_counties)
        ),
        key_columns,
    )
    selected = (
        latest
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
            "_schema_version",
            optional_double(df, "aqi").alias("aqi"),
        )
        .withColumn(
            "event_timestamp",
            # Parse directly as a wall-clock value so host DST rules cannot
            # normalize a local 02:00 observation into 03:00.
            F.to_timestamp_ntz(
                F.concat_ws(
                    " ",
                    F.date_format("date_local", "yyyy-MM-dd"),
                    F.regexp_extract(
                        F.col("time_local").cast("string"),
                        r"(\d{1,2}:\d{2}(?::\d{2})?)$",
                        1,
                    ),
                )
            ),
        )
    )

    valid, rejected = classify_records(
        selected,
        stage="cleaning",
        rejection_rules=[
            ("invalid local date or time", F.col("event_timestamp").isNull()),
            ("sample measurement is required", F.col("pm25_ug_m3").isNull()),
            ("sample measurement must be non-negative", F.col("pm25_ug_m3") < 0),
            ("aqi must be between 0 and 500", F.col("aqi").isNotNull() & ~F.col("aqi").between(0, 500)),
            (
                "unexpected unit of measure",
                F.col("units_of_measure").isNull()
                | (F.col("units_of_measure") != expected_unit),
            ),
        ],
    )

    hourly = (
        valid.groupBy(
            F.col("event_timestamp").alias("event_hour")
        )
        .agg(
            F.avg("pm25_ug_m3").alias("pm25_avg_ug_m3"),
            F.min("pm25_ug_m3").alias("pm25_min_ug_m3"),
            F.max("pm25_ug_m3").alias("pm25_max_ug_m3"),
            F.max("aqi").alias("aqi"),
            F.count("*").alias("air_quality_observation_count"),
            F.countDistinct(
                "county_code",
                "site_num",
            ).alias("air_quality_site_count"),
            F.sort_array(
                F.collect_set("_schema_version")
            ).alias("source_schema_versions"),
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

    result = hourly.select(
        "event_timestamp",
        "event_hour",
        "pm25_avg_ug_m3",
        "pm25_min_ug_m3",
        "pm25_max_ug_m3",
        "aqi",
        "air_quality_observation_count",
        "air_quality_site_count",
        "source_schema_versions",
        "event_year",
        "event_month",
    )
    return result, combine_rejections(incomplete, rejected)


def clean_taxi_trips(
    df: DataFrame,
    dataset_config: dict,
) -> DataFrame:
    cleaned, _ = clean_taxi_trips_with_rejections(df, dataset_config)
    return cleaned


def clean_taxi_trips_with_rejections(
    df: DataFrame,
    dataset_config: dict,
) -> tuple[DataFrame, DataFrame]:
    """Clean taxi trips and derive fields needed for integration."""
    rules = dataset_config["quality_rules"]
    in_period = pickup_period_condition("pickup_timestamp", rules)
    max_duration = rules["max_trip_duration_seconds"]
    max_distance = rules["max_trip_distance"]
    duration_seconds = F.expr(
        "timestampdiff(SECOND, pickup_timestamp, dropoff_timestamp)"
    )

    latest, incomplete = latest_complete_records(df, ["_record_hash"])
    selected = latest.select(
        F.col("_record_hash").alias("trip_id"),
        F.col("_schema_version").alias("source_schema_version"),
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

    with_duration = selected.withColumn(
        "trip_duration_seconds",
        duration_seconds,
    )
    pickup_present = F.col("pickup_timestamp").isNotNull()
    timestamps_present = pickup_present & F.col("dropoff_timestamp").isNotNull()
    valid_trips, rejected = classify_records(
        with_duration,
        stage="cleaning",
        rejection_rules=[
            ("trip_id is required", F.col("trip_id").isNull()),
            ("pickup timestamp is required", F.col("pickup_timestamp").isNull()),
            ("dropoff timestamp is required", F.col("dropoff_timestamp").isNull()),
            ("pickup location is required", F.col("pickup_location_id").isNull()),
            ("dropoff location is required", F.col("dropoff_location_id").isNull()),
            ("trip distance is required", F.col("trip_distance").isNull()),
            (
                "pickup timestamp is outside the configured period",
                pickup_present
                & ~in_period,
            ),
            (
                "trip duration could not be calculated",
                timestamps_present & F.col("trip_duration_seconds").isNull(),
            ),
            (
                "trip duration is outside the allowed range",
                F.col("trip_duration_seconds").isNotNull()
                & (
                    (F.col("trip_duration_seconds") <= 0)
                    | (F.col("trip_duration_seconds") > max_duration)
                ),
            ),
            (
                "trip distance is outside the allowed range",
                F.col("trip_distance").isNotNull()
                & ~F.col("trip_distance").between(0, max_distance),
            ),
        ],
    )

    cleaned = (
        valid_trips
        .withColumn(
            "has_invalid_distance",
            F.lit(False),
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
            # Build an NTZ hour directly; date_trunc returns a zoned timestamp
            # and can shift nonexistent hours in the host machine's timezone.
            F.make_timestamp_ntz(
                F.year("pickup_timestamp"),
                F.month("pickup_timestamp"),
                F.dayofmonth("pickup_timestamp"),
                F.hour("pickup_timestamp"),
                F.lit(0),
                F.lit(0),
            ),
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
    return cleaned, combine_rejections(incomplete, rejected)


def optional_double(df: DataFrame, name: str):
    return F.col(name).cast("double") if name in df.columns else F.lit(None).cast("double")


def normalize_air_times(df: DataFrame) -> DataFrame:
    """Use one key representation for CSV durations and HH:mm[:ss] strings."""
    for column, date_column in [("time_local", "date_local"), ("time_gmt", "date_gmt")]:
        if column in df.columns and date_column in df.columns:
            value = F.regexp_extract(F.col(column), r"(\d{1,2}:\d{2}(?::\d{2})?)$", 1)
            parsed = F.to_timestamp_ntz(F.concat_ws(" ", F.col(date_column).cast("string"), value))
            # Casting NTZ to text preserves wall time; date_format implicitly
            # converts to a zoned timestamp and shifts nonexistent DST hours.
            df = df.withColumn(column, F.coalesce(
                F.substring(parsed.cast("string"), 12, 8), F.col(column).cast("string")))
    return df


def pickup_period_condition(column: str, rules: dict):
    periods = [(rules["pickup_start"], rules["pickup_end_exclusive"])]
    periods.extend(rules.get("additional_pickup_periods", []))
    condition = F.lit(False)
    for start, end in periods:
        condition = condition | ((F.col(column) >= F.lit(start).cast("timestamp_ntz"))
                                 & (F.col(column) < F.lit(end).cast("timestamp_ntz")))
    return condition


def build_normal_tables(spark: SparkSession, config: dict) -> None:
    """Validate, clean, standardize, and write normal Delta tables."""
    zone_config = config["datasets"]["taxi_zone_lookup"]
    zone_raw = read_delta(
        spark,
        table_path(config, zone_config["raw_table"]),
    )
    zone_normal, zone_rejected = clean_taxi_zones_with_rejections(zone_raw)
    write_delta(
        zone_normal,
        table_path(config, zone_config["normal_table"]),
    )
    save_cleaning_rejections(zone_rejected, config, "taxi_zone_lookup")

    weather_config = config["datasets"]["weather_hourly"]
    weather_raw = read_delta(
        spark,
        table_path(config, weather_config["raw_table"]),
    )
    weather_normal, weather_rejected = clean_weather_with_rejections(weather_raw)
    write_delta(
        weather_normal,
        table_path(config, weather_config["normal_table"]),
    )
    save_cleaning_rejections(weather_rejected, config, "weather_hourly")
    air_quality_config = config["datasets"]["air_quality"]
    air_quality_raw = read_delta(
        spark,
        table_path(config, air_quality_config["raw_table"]),
    )
    air_quality_normal, air_quality_rejected = clean_air_quality_with_rejections(
        air_quality_raw
    )
    write_delta(
        air_quality_normal,
        table_path(config, air_quality_config["normal_table"]),
        partitions=air_quality_config.get("partitions"),
    )
    save_cleaning_rejections(air_quality_rejected, config, "air_quality")

    taxi_config = config["datasets"]["yellow_taxi_trips"]
    taxi_raw = read_delta(
        spark,
        table_path(config, taxi_config["raw_table"]),
    )
    taxi_normal, taxi_rejected = clean_taxi_trips_with_rejections(
        taxi_raw,
        taxi_config,
    )
    write_delta(
        taxi_normal,
        table_path(config, taxi_config["normal_table"]),
        partitions=taxi_config.get("partitions"),
    )
    save_cleaning_rejections(taxi_rejected, config, "yellow_taxi_trips")


def save_cleaning_rejections(
    rejected: DataFrame,
    config: dict,
    dataset_name: str,
) -> None:
    rejected_count = write_rejected_records(
        rejected,
        config,
        stage="cleaning",
        dataset_name=dataset_name,
        mode="overwrite",
    )
    print(f"{dataset_name}: wrote {rejected_count} cleaning rejected records")
