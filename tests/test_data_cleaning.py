from datetime import date, datetime

import pytest
from pyspark.sql import SparkSession

from src.data_cleaning.normal_tables import (
    clean_air_quality,
    clean_taxi_trips,
    clean_weather,
    validate_primary_key,
)


def test_duplicate_primary_key_is_rejected(
    spark: SparkSession,
) -> None:
    df = spark.createDataFrame(
        [(1,), (1,)],
        ["location_id"],
    )

    with pytest.raises(ValueError, match="duplicated primary keys"):
        validate_primary_key(
            df,
            ["location_id"],
            "test_zones",
        )


def test_weather_timestamp_is_constructed(
    spark: SparkSession,
) -> None:
    columns = [
        "_schema_version",
        "_ingestion_timestamp",
        "year",
        "month",
        "day",
        "hour",
        "temp",
        "rhum",
        "prcp",
        "snwd",
        "wdir",
        "wspd",
        "wpgt",
        "pres",
        "cldc",
        "coco",
    ]
    values = [
        (
            1,
            datetime(2024, 4, 1),
            2024,
            1,
            2,
            12,
            6.1,
            47,
            0.0,
            "",
            280,
            9.4,
            "",
            1015.8,
            8,
            3,
        )
    ]
    raw = spark.createDataFrame(values, columns)

    result = clean_weather(raw).first()

    assert result.event_timestamp == datetime(2024, 1, 2, 12)
    assert result.event_hour == datetime(2024, 1, 2, 12)
    assert result.temperature_c == 6.1
    assert result.snow_depth_mm is None
    assert result.source_schema_version == 1


def test_air_quality_is_aggregated_by_correct_date(
    spark: SparkSession,
) -> None:
    columns = [
        "_schema_version",
        "_ingestion_timestamp",
        "state_code",
        "state_name",
        "county_name",
        "county_code",
        "site_num",
        "parameter_code",
        "poc",
        "date_local",
        "time_local",
        "sample_measurement",
        "units_of_measure",
    ]
    unit = "Micrograms/cubic meter (LC)"
    values = [
        (
            1,
            datetime(2024, 4, 1),
            36,
            "New York",
            "Queens",
            81,
            1,
            88101,
            1,
            date(2024, 1, 2),
            datetime(2026, 9, 5, 12),
            4.0,
            unit,
        ),
        (
            2,
            datetime(2024, 5, 1),
            36,
            "New York",
            "Kings",
            47,
            2,
            88101,
            1,
            date(2024, 1, 2),
            datetime(2026, 9, 5, 12),
            8.0,
            unit,
        ),
    ]
    raw = spark.createDataFrame(values, columns)

    rows = clean_air_quality(raw).collect()

    assert len(rows) == 1
    assert rows[0].event_hour == datetime(2024, 1, 2, 12)
    assert rows[0].pm25_avg_ug_m3 == 6.0
    assert rows[0].air_quality_observation_count == 2
    assert rows[0].air_quality_site_count == 2
    assert rows[0].source_schema_versions == [1, 2]


def test_taxi_trip_cleaning_filters_flags_and_deduplicates(
    spark: SparkSession,
) -> None:
    base_trip = {
        "_schema_version": 2,
        "_ingestion_timestamp": datetime(2024, 5, 1),
        "_record_hash": "valid-trip",
        "vendor_id": 1,
        "pickup_timestamp": datetime(2024, 1, 2, 12, 0),
        "dropoff_timestamp": datetime(2024, 1, 2, 12, 10),
        "passenger_count": 1,
        "trip_distance": 2.0,
        "ratecode_id": 1,
        "store_and_fwd_flag": "N",
        "pickup_location_id": 1,
        "dropoff_location_id": 2,
        "payment_type": 1,
        "fare_amount": 10.0,
        "extra": 0.0,
        "mta_tax": 0.5,
        "tip_amount": 1.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 1.0,
        "total_amount": 12.5,
        "congestion_surcharge": 0.0,
        "airport_fee": 0.0,
    }

    financial_adjustment = {
        **base_trip,
        "_record_hash": "financial-adjustment",
        "trip_distance": 0.0,
        "fare_amount": -5.0,
        "total_amount": -5.0,
    }
    invalid_distance = {
        **base_trip,
        "_record_hash": "invalid-distance",
        "trip_distance": 600.0,
    }
    invalid_duration = {
        **base_trip,
        "_record_hash": "invalid-duration",
        "dropoff_timestamp": datetime(2024, 1, 2, 12, 0),
    }
    outside_project_period = {
        **base_trip,
        "_record_hash": "outside-project-period",
        "pickup_timestamp": datetime(2023, 12, 31, 12, 0),
        "dropoff_timestamp": datetime(2023, 12, 31, 12, 10),
    }

    raw = spark.createDataFrame(
        [
            base_trip,
            base_trip,
            financial_adjustment,
            invalid_distance,
            invalid_duration,
            outside_project_period,
        ]
    )
    dataset_config = {
        "quality_rules": {
            "pickup_start": "2024-01-01 00:00:00",
            "pickup_end_exclusive": "2024-04-01 00:00:00",
            "max_trip_duration_seconds": 86400,
            "max_trip_distance": 500.0,
        }
    }

    rows = {
        row.trip_id: row
        for row in clean_taxi_trips(raw, dataset_config).collect()
    }

    assert set(rows) == {
        "valid-trip",
        "financial-adjustment",
        "invalid-distance",
    }
    assert rows["valid-trip"].trip_duration_minutes == 10.0
    assert rows["valid-trip"].pickup_hour == datetime(2024, 1, 2, 12)
    assert rows["valid-trip"].source_schema_version == 2
    assert rows["financial-adjustment"].is_zero_distance is True
    assert rows["financial-adjustment"].is_financial_adjustment is True
    assert rows["invalid-distance"].has_invalid_distance is True
    assert rows["invalid-distance"].trip_distance is None


def test_weather_prefers_latest_schema_version_for_same_hour(
    spark: SparkSession,
) -> None:
    columns = [
        "_schema_version",
        "_ingestion_timestamp",
        "year",
        "month",
        "day",
        "hour",
        "temp",
        "rhum",
        "prcp",
        "snwd",
        "wdir",
        "wspd",
        "wpgt",
        "pres",
        "cldc",
        "coco",
    ]
    rows = [
        (1, datetime(2024, 4, 1), 2024, 1, 2, 12, 5.0, 50, 0, 0, 0, 0, 0, 1000, 0, 1),
        (2, datetime(2024, 5, 1), 2024, 1, 2, 12, 6.0, 50, 0, 0, 0, 0, 0, 1000, 0, 1),
    ]

    result = clean_weather(spark.createDataFrame(rows, columns)).first()

    assert result.temperature_c == 6.0
    assert result.source_schema_version == 2
