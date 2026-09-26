from datetime import datetime

from pyspark.sql import SparkSession

from src.data_integration.integrated_tables import (
    integrate_taxi_trips,
    validate_taxi_zone_references,
)


def test_missing_taxi_zone_references_are_quarantined(
    spark: SparkSession,
) -> None:
    trips = spark.createDataFrame(
        [("valid", 1, 2), ("bad-pickup", 99, 2), ("bad-both", 98, 97)],
        "trip_id string, pickup_location_id int, dropoff_location_id int",
    )
    zones = spark.createDataFrame([(1,), (2,)], "location_id int")

    accepted, rejected = validate_taxi_zone_references(trips, zones)

    assert [row.trip_id for row in accepted.collect()] == ["valid"]
    rejected_rows = {row.trip_id: row for row in rejected.collect()}
    assert set(rejected_rows) == {"bad-pickup", "bad-both"}
    assert rejected_rows["bad-pickup"]._validation_rule_ids == [
        "reference.pickup_location_id"
    ]
    assert set(rejected_rows["bad-both"]._validation_rule_ids) == {
        "reference.pickup_location_id",
        "reference.dropoff_location_id",
    }


def test_integration_preserves_unmatched_taxi_trips(
    spark: SparkSession,
) -> None:
    hour_12 = datetime(2024, 1, 2, 12)
    hour_13 = datetime(2024, 1, 2, 13)

    taxi_trips = spark.createDataFrame(
        [
            ("trip-a", 1, 2, hour_12, 2),
            ("trip-b", 2, 1, hour_13, 2),
        ],
        [
            "trip_id",
            "pickup_location_id",
            "dropoff_location_id",
            "pickup_hour",
            "source_schema_version",
        ],
    )

    zones = spark.createDataFrame(
        [
            (1, "Manhattan", "Zone A", "Yellow Zone", 1),
            (2, "Queens", "Zone B", "Boro Zone", 1),
        ],
        [
            "location_id",
            "borough",
            "zone",
            "service_zone",
            "source_schema_version",
        ],
    )

    weather = spark.createDataFrame(
        [
            (
                hour_12,
                5.0,
                50.0,
                0.0,
                0.0,
                180.0,
                10.0,
                15.0,
                1015.0,
                20.0,
                1,
                2,
            )
        ],
        [
            "event_hour",
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
        ],
    )

    air_quality = spark.createDataFrame(
        [
            (
                hour_12,
                6.0,
                4.0,
                8.0,
                5,
                5,
                [1, 2],
            )
        ],
        [
            "event_hour",
            "pm25_avg_ug_m3",
            "pm25_min_ug_m3",
            "pm25_max_ug_m3",
            "air_quality_observation_count",
            "air_quality_site_count",
            "source_schema_versions",
        ],
    )

    rows = (
        integrate_taxi_trips(
            taxi_trips,
            zones,
            weather,
            air_quality,
        )
        .orderBy("trip_id")
        .collect()
    )

    assert len(rows) == 2

    matched = rows[0]
    assert matched.trip_id == "trip-a"
    assert matched.pickup_borough == "Manhattan"
    assert matched.dropoff_borough == "Queens"
    assert matched.weather_available is True
    assert matched.air_quality_available is True
    assert matched.taxi_schema_version == 2
    assert matched.pickup_zone_schema_version == 1
    assert matched.dropoff_zone_schema_version == 1
    assert matched.weather_schema_version == 2
    assert matched.air_quality_schema_versions == [1, 2]

    unmatched = rows[1]
    assert unmatched.trip_id == "trip-b"
    assert unmatched.weather_available is False
    assert unmatched.air_quality_available is False
    assert unmatched.weather_schema_version is None
    assert unmatched.air_quality_schema_versions is None
