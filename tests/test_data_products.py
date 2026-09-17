from copy import deepcopy
from datetime import date, datetime

from src.common import load_config, read_delta, table_path, write_delta
from src.data_analysis.data_products import (
    air_quality_impact_summary,
    build_data_products,
    daily_mobility_summary,
    taxi_zone_statistics,
    weather_impact_summary,
)
from src.data_analysis.weather_conditions import weather_condition_name


INTEGRATED_SCHEMA = """
    trip_id string,
    pickup_timestamp timestamp_ntz,
    pickup_date date,
    pickup_year int,
    pickup_month int,
    passenger_count bigint,
    trip_distance double,
    trip_duration_seconds long,
    fare_amount double,
    total_amount double,
    pickup_location_id int,
    pickup_zone string,
    pickup_borough string,
    weather_condition_code int,
    pm25_avg_ug_m3 double,
    taxi_schema_version int,
    pickup_zone_schema_version int,
    dropoff_zone_schema_version int,
    weather_schema_version int,
    air_quality_schema_versions array<int>
"""


def integrated_fixture(spark):
    return spark.createDataFrame(
        [
            ("a", datetime(2024, 1, 1, 10), date(2024, 1, 1), 2024, 1,
             1, 2.0, 600, 10.0, 12.0, 1, "A", "Manhattan", 1, 5.0,
             1, 1, 1, 1, [1]),
            ("b", datetime(2024, 1, 1, 10, 30), date(2024, 1, 1), 2024, 1,
             2, 4.0, 1200, 20.0, 24.0, 1, "A", "Manhattan", 1, 8.0,
             2, 1, 1, 1, [1, 2]),
            ("c", datetime(2024, 1, 1, 12), date(2024, 1, 1), 2024, 1,
             1, 6.0, 1800, 30.0, 36.0, 2, "B", "Queens", 2, 40.0,
             1, 2, 1, 2, [2]),
            ("d", datetime(2024, 2, 1, 8), date(2024, 2, 1), 2024, 2,
             1, 8.0, 2400, 40.0, 48.0, 1, "A", "Manhattan", 1, 200.0,
             1, 1, 1, 1, [1]),
        ],
        INTEGRATED_SCHEMA,
    )


def test_four_product_builders_have_expected_grain_and_metrics(spark) -> None:
    integrated = integrated_fixture(spark)

    daily = {row.pickup_date: row for row in daily_mobility_summary(integrated).collect()}
    assert daily[date(2024, 1, 1)].trip_count == 3
    assert daily[date(2024, 1, 1)].passenger_count == 4
    assert daily[date(2024, 1, 1)].peak_hour == 10
    assert daily[date(2024, 1, 1)].peak_hour_trip_count == 2
    assert daily[date(2024, 1, 1)].taxi_schema_versions == [1, 2]

    zones = taxi_zone_statistics(integrated).collect()
    zone_a_january = next(
        row for row in zones
        if row.pickup_month == 1 and row.pickup_location_id == 1
    )
    assert zone_a_january.trip_count == 2
    assert zone_a_january.avg_trip_distance == 3.0

    weather = weather_impact_summary(integrated).collect()
    sunny_january = next(
        row for row in weather
        if row.pickup_month == 1 and row.weather_condition_code == 1
    )
    assert sunny_january.weather_condition == "Clear"
    assert sunny_january.trip_count == 2
    assert sunny_january.avg_trip_distance == 3.0

    air = air_quality_impact_summary(integrated).collect()
    categories = {(row.pickup_month, row.air_quality_category): row for row in air}
    assert categories[(1, "good")].trip_count == 2
    assert categories[(1, "unhealthy_sensitive")].trip_count == 1
    assert categories[(2, "very_unhealthy")].trip_count == 1


def test_weather_condition_names_follow_meteostat_codes() -> None:
    assert weather_condition_name(1) == "Clear"
    assert weather_condition_name(7) == "Light Rain"
    assert weather_condition_name(25) == "Thunderstorm"
    assert weather_condition_name(99) == "Unknown (99)"


def test_product_refresh_writes_metadata_and_preserves_catalog_history(spark, tmp_path) -> None:
    config = deepcopy(load_config())
    config["paths"]["lakehouse"] = str(tmp_path / "lakehouse")
    source_relative = config["data_integration"]["integrated_taxi_trips_table"]
    write_delta(integrated_fixture(spark), table_path(config, source_relative))

    first = build_data_products(spark, config)
    assert len(first) == 4
    catalog_path = table_path(config, config["data_analysis"]["products"]["catalog_table"])
    first_catalog = {
        row.product_name: row.asDict(recursive=True)
        for row in read_delta(spark, catalog_path).collect()
    }
    assert len(first_catalog) == 4
    assert all(record["row_count"] > 0 for record in first_catalog.values())
    assert all(record["storage_bytes"] > 0 for record in first_catalog.values())

    product_name = "daily_mobility_summary"
    product_definition = config["data_analysis"]["products"]["definitions"][product_name]
    product = read_delta(spark, table_path(config, product_definition["table"]))
    metadata = product.select(
        "_product_name",
        "_product_schema_version",
        "_source_table",
        "_source_delta_version",
        "_source_schema_versions",
        "_created_at",
        "_refreshed_at",
    ).first()
    assert metadata._product_name == product_name
    assert metadata._product_schema_version == 1
    assert metadata._source_delta_version == 0
    assert '"taxi_schema_versions": [1, 2]' in metadata._source_schema_versions

    build_data_products(spark, config, product_name)
    second_catalog = {
        row.product_name: row.asDict(recursive=True)
        for row in read_delta(spark, catalog_path).collect()
    }
    assert len(second_catalog) == 4
    assert second_catalog[product_name]["created_at"] == first_catalog[product_name]["created_at"]
    assert second_catalog[product_name]["refreshed_at"] >= first_catalog[product_name]["refreshed_at"]
