from copy import deepcopy
from datetime import datetime

import pytest
from pyspark.sql import functions as F

from src.common import load_config, read_delta, table_path, write_delta
from src.machine_learning.training_dataset import (
    build_training_dataset,
    hourly_taxi_demand_dataset,
)


INTEGRATED_ML_SCHEMA = """
    pickup_hour timestamp,
    pickup_location_id int,
    pickup_zone string,
    pickup_borough string,
    pickup_service_zone string,
    temperature_c double,
    relative_humidity_pct double,
    precipitation_mm double,
    snow_depth_mm double,
    wind_direction_deg double,
    wind_speed_kmh double,
    wind_gust_kmh double,
    pressure_hpa double,
    cloud_cover_pct double,
    weather_condition_code int,
    pm25_avg_ug_m3 double,
    pm25_min_ug_m3 double,
    pm25_max_ug_m3 double,
    air_quality_observation_count long,
    air_quality_site_count long,
    weather_available boolean,
    air_quality_available boolean
"""


def integrated_row(hour: int, location_id: int):
    return (
        datetime(2024, 1, 1, hour),
        location_id,
        f"Zone {location_id}",
        "Manhattan" if location_id == 1 else "Queens",
        "Yellow Zone" if location_id == 1 else "Boro Zone",
        float(hour),
        50.0,
        0.0,
        0.0,
        180.0,
        10.0,
        15.0,
        1015.0,
        20.0,
        1,
        6.0,
        4.0,
        8.0,
        5,
        2,
        True,
        True,
    )


def ml_config() -> dict:
    return {
        "prediction_task": "hourly_taxi_demand",
        "source_table": "integrated/integrated_taxi_trips",
        "training_dataset_table": "ml/hourly_taxi_demand",
        "label_column": "demand",
        "output_partitions": ["split"],
        "observation_start": "2024-01-01 10:00:00",
        "train_end_exclusive": "2024-01-01 11:00:00",
        "validation_end_exclusive": "2024-01-01 12:00:00",
        "observation_end_exclusive": "2024-01-01 13:00:00",
    }


def integrated_fixture(spark):
    return spark.createDataFrame(
        [
            integrated_row(10, 1),
            integrated_row(10, 1),
            integrated_row(10, 2),
            integrated_row(11, 1),
            integrated_row(12, 2),
            # Outside the configured observation period and must not add zone 3.
            integrated_row(13, 3),
        ],
        INTEGRATED_ML_SCHEMA,
    )


def test_hourly_dataset_preserves_zero_demand_and_time_splits(spark) -> None:
    dataset = hourly_taxi_demand_dataset(integrated_fixture(spark), ml_config())

    rows = {
        (row.pickup_hour.hour, row.pickup_location_id): row
        for row in dataset.collect()
    }
    assert len(rows) == 6
    assert rows[(10, 1)].demand == 2
    assert rows[(10, 2)].demand == 1
    assert rows[(11, 1)].demand == 1
    assert rows[(11, 2)].demand == 0
    assert rows[(12, 1)].demand == 0
    assert rows[(12, 2)].demand == 1
    assert rows[(10, 1)].split == "train"
    assert rows[(11, 1)].split == "validation"
    assert rows[(12, 1)].split == "test"
    assert rows[(11, 2)].pickup_zone == "Zone 2"
    assert rows[(11, 2)].temperature_c == 11.0
    assert sum(row.demand for row in rows.values()) == 5


def test_hourly_dataset_has_one_row_per_zone_hour(spark) -> None:
    dataset = hourly_taxi_demand_dataset(integrated_fixture(spark), ml_config())

    duplicate_keys = (
        dataset
        .groupBy("pickup_hour", "pickup_location_id")
        .count()
        .filter(F.col("count") != 1)
        .count()
    )
    split_counts = {
        row.split: row["count"]
        for row in dataset.groupBy("split").count().collect()
    }
    assert duplicate_keys == 0
    assert split_counts == {"train": 2, "validation": 2, "test": 2}
    assert dataset.filter(F.col("demand") < 0).count() == 0


def test_hourly_dataset_rejects_incomplete_integrated_contract(spark) -> None:
    incomplete = spark.createDataFrame(
        [(datetime(2024, 1, 1, 10), 1)],
        "pickup_hour timestamp, pickup_location_id int",
    )

    with pytest.raises(ValueError, match="missing required columns"):
        hourly_taxi_demand_dataset(incomplete, ml_config())


def test_build_training_dataset_reads_and_writes_delta(spark, tmp_path) -> None:
    config = deepcopy(load_config())
    config["paths"]["lakehouse"] = str(tmp_path / "lakehouse")
    config["machine_learning"] = ml_config()

    source = table_path(config, config["machine_learning"]["source_table"])
    output = table_path(
        config,
        config["machine_learning"]["training_dataset_table"],
    )
    write_delta(integrated_fixture(spark), source)

    generated = build_training_dataset(spark, config)
    persisted = read_delta(spark, output)

    assert generated.count() == 6
    assert persisted.count() == 6
    assert {
        row.split
        for row in persisted.select("split").distinct().collect()
    } == {
        "train",
        "validation",
        "test",
    }
