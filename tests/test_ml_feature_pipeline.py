from copy import deepcopy
from datetime import datetime
import math
from pathlib import Path

import pytest
from pyspark.ml import PipelineModel
from pyspark.ml.linalg import VectorUDT

from src.common import load_config, read_delta, table_path, write_delta
from src.machine_learning.feature_pipeline import (
    fit_feature_pipeline,
    prepare_feature_dataset,
    require_feature_source_columns,
    transform_feature_dataset,
)


FEATURE_SOURCE_SCHEMA = """
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
    air_quality_available boolean,
    demand long,
    split string
"""


def feature_row(
    timestamp: datetime,
    location_id: int,
    split: str,
    *,
    temperature: float | None = 10.0,
    borough: str = "Manhattan",
    service_zone: str = "Yellow Zone",
    weather_code: int = 1,
    available: bool = True,
):
    return (
        timestamp,
        location_id,
        f"Zone {location_id}",
        borough,
        service_zone,
        temperature,
        50.0,
        0.0,
        0.0,
        180.0,
        10.0,
        15.0,
        1015.0,
        20.0,
        weather_code,
        6.0,
        4.0,
        8.0,
        5,
        2,
        available,
        available,
        3,
        split,
    )


def feature_source(spark):
    return spark.createDataFrame(
        [
            feature_row(datetime(2024, 1, 1, 8), 1, "train"),
            feature_row(
                datetime(2024, 1, 2, 9),
                2,
                "train",
                temperature=None,
                borough="Queens",
                service_zone="Boro Zone",
                weather_code=2,
            ),
            # Unseen location, borough, service zone, and weather code.
            feature_row(
                datetime(2024, 1, 6, 10),
                99,
                "validation",
                borough="Bronx",
                service_zone="Airports",
                weather_code=9,
                available=False,
            ),
            feature_row(datetime(2024, 2, 1, 11), 1, "test"),
        ],
        FEATURE_SOURCE_SCHEMA,
    )


def test_pipeline_derives_imputes_encodes_and_scales(spark) -> None:
    config = load_config()
    source = feature_source(spark)

    model = fit_feature_pipeline(source, config)
    expanded = model.transform(source)
    prepared = transform_feature_dataset(source, model, config)

    saturday = expanded.filter("pickup_location_id = 99").first()
    imputed = expanded.filter("pickup_location_id = 2").first()
    assert saturday.hour_of_day == 10.0
    assert saturday.day_of_week == 7.0
    assert saturday.month == 1.0
    assert saturday.is_weekend == 1.0
    assert saturday.weather_available_flag == 0.0
    assert saturday.air_quality_available_flag == 0.0
    assert imputed.temperature_c__imputed == 10.0

    assert prepared.columns == [
        "pickup_hour",
        "pickup_location_id",
        "demand",
        "split",
        "features",
    ]
    vectors = [row.features for row in prepared.select("features").collect()]
    assert len({vector.size for vector in vectors}) == 1
    assert vectors[0].size > len(config["machine_learning"]["numerical_features"])
    assert all(
        math.isfinite(value)
        for vector in vectors
        for value in vector.toArray()
    )
    assert isinstance(prepared.schema["features"].dataType, VectorUDT)


def test_pipeline_learns_categories_only_from_train_split(spark) -> None:
    config = load_config()
    source = feature_source(spark)

    model = fit_feature_pipeline(source, config)
    indexer_model = next(
        stage for stage in model.stages
        if hasattr(stage, "labelsArray")
    )
    pickup_location_labels = set(indexer_model.labelsArray[0])

    assert "99" not in pickup_location_labels
    assert "99.0" not in pickup_location_labels
    assert transform_feature_dataset(source, model, config).count() == 4


def test_feature_pipeline_rejects_incomplete_task1_contract(spark) -> None:
    config = load_config()
    incomplete = spark.createDataFrame(
        [(datetime(2024, 1, 1, 8), 1, 2, "train")],
        "pickup_hour timestamp, pickup_location_id int, demand long, split string",
    )

    with pytest.raises(ValueError, match="missing feature-pipeline columns"):
        require_feature_source_columns(
            incomplete,
            config["machine_learning"],
        )


def test_prepare_feature_dataset_persists_delta_and_model(
    spark,
    tmp_path,
) -> None:
    config = deepcopy(load_config())
    config["paths"]["lakehouse"] = str(tmp_path / "lakehouse")
    machine_learning = config["machine_learning"]

    source_path = table_path(
        config,
        machine_learning["training_dataset_table"],
    )
    output_path = table_path(
        config,
        machine_learning["prepared_dataset_table"],
    )
    model_path = table_path(
        config,
        machine_learning["feature_pipeline_model_path"],
    )
    source = feature_source(spark)
    write_delta(source, source_path, partitions=["split"])

    prepared, _ = prepare_feature_dataset(spark, config)
    persisted = read_delta(spark, output_path)
    loaded_model = PipelineModel.load(model_path)

    assert prepared.count() == 4
    assert persisted.count() == 4
    assert Path(model_path).is_dir()
    assert loaded_model.transform(source).count() == 4
