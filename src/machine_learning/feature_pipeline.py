"""Fit and apply the reusable Week 4 Spark ML feature pipeline."""
from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import (
    Imputer,
    OneHotEncoder,
    SQLTransformer,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import (
    create_spark,
    load_config,
    read_delta,
    table_path,
    write_delta,
)


DERIVED_FEATURE_COLUMNS = {
    "hour_of_day",
    "day_of_week",
    "month",
    "is_weekend",
    "weather_available_flag",
    "air_quality_available_flag",
}

DERIVED_FEATURE_DEPENDENCIES = {
    "pickup_hour",
    "weather_available",
    "air_quality_available",
}

NUMERIC_VECTOR_COLUMN = "numeric_features_unscaled"
SCALED_NUMERIC_VECTOR_COLUMN = "numeric_features_scaled"

TEMPORAL_FEATURE_STATEMENT = """
SELECT
  *,
  CAST(hour(pickup_hour) AS DOUBLE) AS hour_of_day,
  CAST(dayofweek(pickup_hour) AS DOUBLE) AS day_of_week,
  CAST(month(pickup_hour) AS DOUBLE) AS month,
  CASE
    WHEN dayofweek(pickup_hour) IN (1, 7) THEN 1.0
    ELSE 0.0
  END AS is_weekend,
  CASE WHEN COALESCE(weather_available, FALSE) THEN 1.0 ELSE 0.0 END
    AS weather_available_flag,
  CASE WHEN COALESCE(air_quality_available, FALSE) THEN 1.0 ELSE 0.0 END
    AS air_quality_available_flag
FROM __THIS__
"""


def feature_column_names(machine_learning: dict) -> dict[str, list[str]]:
    """Return deterministic intermediate column names for configured features."""
    categorical = machine_learning["categorical_features"]
    numerical = machine_learning["numerical_features"]
    return {
        "imputed": [f"{column}__imputed" for column in numerical],
        "indexed": [f"{column}__index" for column in categorical],
        "encoded": [f"{column}__encoded" for column in categorical],
    }


def require_feature_source_columns(
    dataset: DataFrame,
    machine_learning: dict,
) -> None:
    """Validate the Task 1 dataset before fitting Spark ML transformers."""
    configured_features = set(machine_learning["categorical_features"]) | set(
        machine_learning["numerical_features"]
    )
    required = (
        configured_features - DERIVED_FEATURE_COLUMNS
    ) | DERIVED_FEATURE_DEPENDENCIES | {
        machine_learning["label_column"],
        "split",
        *machine_learning["output_metadata_columns"],
    }
    missing = sorted(required - set(dataset.columns))
    if missing:
        raise ValueError(
            "Training dataset is missing feature-pipeline columns: "
            f"{missing}"
        )


def build_feature_pipeline(config: dict) -> Pipeline:
    """Build an unfitted, configuration-driven Spark ML feature pipeline."""
    machine_learning = config["machine_learning"]
    categorical = machine_learning["categorical_features"]
    numerical = machine_learning["numerical_features"]
    features_column = machine_learning["features_column"]
    names = feature_column_names(machine_learning)

    temporal_features = SQLTransformer(
        statement=TEMPORAL_FEATURE_STATEMENT,
    )
    numerical_imputer = Imputer(
        strategy="median",
        inputCols=numerical,
        outputCols=names["imputed"],
    )
    numerical_assembler = VectorAssembler(
        inputCols=names["imputed"],
        outputCol=NUMERIC_VECTOR_COLUMN,
        handleInvalid="error",
    )
    numerical_scaler = StandardScaler(
        inputCol=NUMERIC_VECTOR_COLUMN,
        outputCol=SCALED_NUMERIC_VECTOR_COLUMN,
        withMean=False,
        withStd=True,
    )
    categorical_indexer = StringIndexer(
        inputCols=categorical,
        outputCols=names["indexed"],
        handleInvalid="keep",
        stringOrderType="alphabetAsc",
    )
    categorical_encoder = OneHotEncoder(
        inputCols=names["indexed"],
        outputCols=names["encoded"],
        handleInvalid="keep",
        dropLast=False,
    )
    final_assembler = VectorAssembler(
        inputCols=[SCALED_NUMERIC_VECTOR_COLUMN, *names["encoded"]],
        outputCol=features_column,
        handleInvalid="error",
    )

    return Pipeline(
        stages=[
            temporal_features,
            numerical_imputer,
            numerical_assembler,
            numerical_scaler,
            categorical_indexer,
            categorical_encoder,
            final_assembler,
        ]
    )


def fit_feature_pipeline(
    dataset: DataFrame,
    config: dict,
) -> PipelineModel:
    """Fit all learned preprocessing state on the training split only."""
    machine_learning = config["machine_learning"]
    require_feature_source_columns(dataset, machine_learning)
    training = dataset.filter(F.col("split") == "train")
    if training.limit(1).count() == 0:
        raise ValueError("Training dataset must contain at least one train row")
    return build_feature_pipeline(config).fit(training)


def transform_feature_dataset(
    dataset: DataFrame,
    model: PipelineModel,
    config: dict,
) -> DataFrame:
    """Apply one fitted transformer and retain only model-facing columns."""
    machine_learning = config["machine_learning"]
    transformed = model.transform(dataset)
    output_columns = list(dict.fromkeys([
        *machine_learning["output_metadata_columns"],
        machine_learning["label_column"],
        "split",
        machine_learning["features_column"],
    ]))
    return transformed.select(*output_columns)


def prepare_feature_dataset(
    spark: SparkSession,
    config: dict,
) -> tuple[DataFrame, PipelineModel]:
    """Fit, save and apply the feature pipeline to the Task 1 Delta table."""
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

    source = read_delta(spark, source_path)
    model = fit_feature_pipeline(source, config)
    model.write().overwrite().save(model_path)

    prepared = transform_feature_dataset(source, model, config)
    write_delta(
        prepared,
        output_path,
        partitions=machine_learning.get("output_partitions"),
    )
    return read_delta(spark, output_path), model


def print_feature_summary(dataset: DataFrame, config: dict) -> None:
    """Print final vector width and row counts for each chronological split."""
    features_column = config["machine_learning"]["features_column"]
    first = dataset.select(features_column).first()
    vector_size = first[features_column].size if first is not None else 0
    print(f"Feature vector size: {vector_size}")
    dataset.groupBy("split").count().orderBy(
        F.when(F.col("split") == "train", 1)
        .when(F.col("split") == "validation", 2)
        .otherwise(3)
    ).show(truncate=False)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/config.yaml"),
    )
    args = parser.parse_args()

    config = load_config(args.config)
    spark = create_spark()
    try:
        prepared, _ = prepare_feature_dataset(spark, config)
        machine_learning = config["machine_learning"]
        print(
            "Created model-ready feature dataset: "
            f"{table_path(config, machine_learning['prepared_dataset_table'])}"
        )
        print(
            "Saved fitted feature pipeline: "
            f"{table_path(config, machine_learning['feature_pipeline_model_path'])}"
        )
        print_feature_summary(prepared, config)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
