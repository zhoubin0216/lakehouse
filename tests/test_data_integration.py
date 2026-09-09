from datetime import datetime

from pyspark.sql import SparkSession

from src.data_integration.integrated_tables import integrate_taxi_trips


def test_integration_preserves_unmatched_taxi_trips(
    spark: SparkSession,
) -> None:
    hour_12 = datetime(2024, 1, 2, 12)
    hour_13 = datetime(2024, 1, 2, 13)

    taxi_trips = spark.createDataFrame(
        [
            ("trip-a", 1, 2, hour_12),
            ("trip-b", 2, 1, hour_13),
        ],
        [
            "trip_id",
            "pickup_location_id",
            "dropoff_location_id",
            "pickup_hour",
        ],
    )

    zones = spark.createDataFrame(
        [
            (1, "Manhattan", "Zone A", "Yellow Zone"),
            (2, "Queens", "Zone B", "Boro Zone"),
        ],
        [
            "location_id",
            "borough",
            "zone",
            "service_zone",
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
            )
        ],
        [
            "event_hour",
            "pm25_avg_ug_m3",
            "pm25_min_ug_m3",
            "pm25_max_ug_m3",
            "air_quality_observation_count",
            "air_quality_site_count",
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

    unmatched = rows[1]
    assert unmatched.trip_id == "trip-b"
    assert unmatched.weather_available is False
    assert unmatched.air_quality_available is False