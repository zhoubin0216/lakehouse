from __future__ import annotations

import time
import os
import sys
from pathlib import Path
from typing import Callable, TypeVar

import yaml
from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession


CONFIG_PATH = Path("configs/config.yaml")
DEFAULT_JAVA_HOME = Path("/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home")
DEFAULT_SPARK_DRIVER_MEMORY = "6g"
DEFAULT_SPARK_LOCAL_THREADS = "4"
SUPPORTED_SOURCE_TYPES = {
    "bigint",
    "boolean",
    "date",
    "double",
    "int",
    "string",
    "timestamp",
    "timestamp_ntz",
}
REQUIRED_DATA_PRODUCTS = {
    "daily_mobility_summary",
    "taxi_zone_statistics",
    "weather_impact_summary",
    "air_quality_impact_summary",
}
T = TypeVar("T")


def load_config(path: Path = CONFIG_PATH) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    validate_config(config)
    return config


def validate_config(config: dict) -> None:
    """Validate configuration fields shared by all pipeline steps."""
    if not isinstance(config, dict):
        raise ValueError("Configuration must be a YAML mapping")

    data_quality = config.get("data_quality")
    if not isinstance(data_quality, dict) or not data_quality.get("rejected_table_root"):
        raise ValueError("Configuration must define data_quality.rejected_table_root")
    summary_table = data_quality.get("validation_summary_table")
    if summary_table is not None and (
        not isinstance(summary_table, str) or not summary_table.strip()
    ):
        raise ValueError(
            "data_quality.validation_summary_table must be a non-empty relative path"
        )

    datasets = config.get("datasets")
    if not isinstance(datasets, dict) or not datasets:
        raise ValueError("Configuration must define at least one dataset")

    for dataset_name, dataset_config in datasets.items():
        if not isinstance(dataset_config, dict):
            raise ValueError(f"Dataset '{dataset_name}' configuration must be a YAML mapping")
        current_version = dataset_config.get("current_schema_version")
        if isinstance(current_version, bool) or not isinstance(current_version, int) or current_version < 1:
            raise ValueError(
                f"Dataset '{dataset_name}' must define current_schema_version as a positive integer"
            )

        schema_versions = dataset_config.get("schema_versions")
        if not isinstance(schema_versions, dict) or not schema_versions:
            raise ValueError(f"Dataset '{dataset_name}' must define schema_versions")

        for version_key, schema_definition in schema_versions.items():
            if not isinstance(version_key, str) or not version_key.isdigit() or int(version_key) < 1:
                raise ValueError(
                    f"Dataset '{dataset_name}' schema_versions keys must be quoted positive integers"
                )
            validate_schema_definition(dataset_name, version_key, schema_definition)

        resolve_schema_definition(dataset_name, dataset_config)
        source_version = dataset_config.get("source_schema_version")
        if source_version is not None:
            if isinstance(source_version, bool) or not isinstance(source_version, int) or source_version < 1:
                raise ValueError(f"Dataset '{dataset_name}' source_schema_version must be a positive integer")
            resolve_schema_definition(dataset_name, dataset_config, source_version)

    validate_data_products_config(config)
    validate_monitoring_config(config)
    validate_evaluation_config(config)


def validate_evaluation_config(config: dict) -> None:
    """Validate optional Week 3 production-evaluation settings."""
    evaluation = config.get("evaluation")
    if evaluation is None:
        return
    if not isinstance(evaluation, dict):
        raise ValueError("evaluation must be a YAML mapping")
    root = evaluation.get("results_root")
    if not isinstance(root, str) or not root.strip():
        raise ValueError("evaluation.results_root must be a non-empty path")
    rows = evaluation.get("synthetic_rows")
    if isinstance(rows, bool) or not isinstance(rows, int) or rows < 100:
        raise ValueError("evaluation.synthetic_rows must be an integer of at least 100")
    repeats = evaluation.get("repeats")
    if isinstance(repeats, bool) or not isinstance(repeats, int) or repeats < 1:
        raise ValueError("evaluation.repeats must be a positive integer")


def validate_monitoring_config(config: dict) -> None:
    """Validate optional Week 3 monitoring configuration."""

    monitoring = config.get("monitoring")

    if monitoring is None:
        return

    if not isinstance(monitoring, dict):
        raise ValueError(
            "monitoring must be a YAML mapping"
        )

    if not isinstance(
        monitoring.get("enabled", True),
        bool,
    ):
        raise ValueError(
            "monitoring.enabled must be true or false"
        )

    for key in (
        "pipeline_runs_table",
        "schema_events_table",
    ):
        value = monitoring.get(key)

        if (
            not isinstance(value, str)
            or not value.strip()
        ):
            raise ValueError(
                f"monitoring.{key} must be "
                "a non-empty relative table path"
            )
def validate_data_products_config(config: dict) -> None:
    """Validate Task 4 analytical product contracts when configured."""
    analysis = config.get("data_analysis")
    if analysis is None:
        return
    if not isinstance(analysis, dict):
        raise ValueError("data_analysis must be a YAML mapping")
    products = analysis.get("products")
    if products is None:
        return
    if not isinstance(products, dict) or not products.get("catalog_table"):
        raise ValueError("data_analysis.products must define catalog_table")
    definitions = products.get("definitions")
    if not isinstance(definitions, dict):
        raise ValueError("data_analysis.products must define product definitions")
    missing = sorted(REQUIRED_DATA_PRODUCTS - set(definitions))
    if missing:
        raise ValueError(f"Missing required analytical products: {missing}")

    required_fields = {
        "table",
        "schema_version",
        "partitions",
        "grain",
        "intended_users",
        "description",
        "materialization_reason",
    }
    for product_name, definition in definitions.items():
        if not isinstance(definition, dict):
            raise ValueError(f"Product '{product_name}' definition must be a YAML mapping")
        missing_fields = sorted(required_fields - set(definition))
        if missing_fields:
            raise ValueError(f"Product '{product_name}' is missing fields: {missing_fields}")
        version = definition["schema_version"]
        if isinstance(version, bool) or not isinstance(version, int) or version < 1:
            raise ValueError(f"Product '{product_name}' schema_version must be a positive integer")
        if not isinstance(definition["partitions"], list):
            raise ValueError(f"Product '{product_name}' partitions must be a list")

    report = analysis.get("report")
    if report is not None:
        if not isinstance(report, dict):
            raise ValueError("data_analysis.report must be a YAML mapping")
        output_file = report.get("output_file")
        if not isinstance(output_file, str) or not output_file.strip():
            raise ValueError("data_analysis.report must define output_file")


def validate_schema_definition(dataset_name: str, version_key: str, schema_definition: dict) -> None:
    """Validate one immutable source schema contract."""
    label = f"Dataset '{dataset_name}' schema version {version_key}"
    if not isinstance(schema_definition, dict):
        raise ValueError(f"{label} must be a YAML mapping")
    if schema_definition.get("format") not in {"csv", "parquet"}:
        raise ValueError(f"{label} must define format as 'csv' or 'parquet'")
    if not isinstance(schema_definition.get("expected_columns"), list):
        raise ValueError(f"{label} must define expected_columns as a list")
    if not isinstance(schema_definition.get("columns"), dict):
        raise ValueError(f"{label} must define columns as a mapping")
    column_types = schema_definition.get("column_types")
    if not isinstance(column_types, dict):
        raise ValueError(f"{label} must define column_types as a mapping")
    expected_columns = set(schema_definition["expected_columns"])
    type_columns = set(column_types)
    if expected_columns != type_columns:
        missing = sorted(expected_columns - type_columns)
        unexpected = sorted(type_columns - expected_columns)
        raise ValueError(
            f"{label} column_types keys must match expected_columns; "
            f"missing={missing}, unexpected={unexpected}"
        )
    unsupported_types = sorted(set(column_types.values()) - SUPPORTED_SOURCE_TYPES)
    if unsupported_types:
        raise ValueError(f"{label} contains unsupported column types: {unsupported_types}")
    if "read_options" in schema_definition and not isinstance(schema_definition["read_options"], dict):
        raise ValueError(f"{label} read_options must be a mapping")
    if schema_definition["format"] == "csv" and schema_definition.get("read_options", {}).get("inferSchema"):
        raise ValueError(f"{label} must disable CSV inferSchema for strict type validation")


def resolve_schema_definition(
    dataset_name: str,
    dataset_config: dict,
    schema_version: int | None = None,
) -> tuple[int, dict]:
    """Return a requested schema definition, defaulting to the current pointer."""
    version = dataset_config.get("current_schema_version") if schema_version is None else schema_version
    definitions = dataset_config.get("schema_versions", {})
    definition = definitions.get(str(version))
    if definition is None:
        raise ValueError(f"Dataset '{dataset_name}' has no schema definition for version {version}")
    return version, definition


def create_spark() -> SparkSession:
    configure_java_home()
    configure_pyspark_python()
    configure_pyspark_submit_args()
    driver_memory = os.environ.get("SPARK_DRIVER_MEMORY", DEFAULT_SPARK_DRIVER_MEMORY)
    local_threads = os.environ.get("SPARK_LOCAL_THREADS", DEFAULT_SPARK_LOCAL_THREADS)
    builder = (
        SparkSession.builder.appName("week1-lakehouse")
        .master(f"local[{local_threads}]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", driver_memory)
        .config("spark.executor.memory", driver_memory)
        .config("spark.default.parallelism", local_threads)
        .config("spark.sql.shuffle.partitions", local_threads)
        .config("spark.sql.files.maxPartitionBytes", "64m")
        .config("spark.hadoop.parquet.block.size", "67108864")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def configure_java_home() -> None:
    if os.environ.get("JAVA_HOME"):
        return
    if not DEFAULT_JAVA_HOME.exists():
        return

    os.environ["JAVA_HOME"] = str(DEFAULT_JAVA_HOME)
    os.environ["PATH"] = f"{DEFAULT_JAVA_HOME / 'bin'}:{os.environ.get('PATH', '')}"


def configure_pyspark_python() -> None:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def configure_pyspark_submit_args() -> None:
    if os.environ.get("PYSPARK_SUBMIT_ARGS"):
        return

    driver_memory = os.environ.get("SPARK_DRIVER_MEMORY", DEFAULT_SPARK_DRIVER_MEMORY)
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--driver-memory {driver_memory} pyspark-shell"


def raw_path(config: dict, dataset: dict) -> str:
    return str(Path(config["paths"]["raw"]) / dataset["source"])


def table_path(config: dict, relative_path: str) -> str:
    return str(Path(config["paths"]["lakehouse"]) / relative_path)


def read_source(
    spark: SparkSession,
    config: dict,
    dataset: dict,
    dataset_name: str = "dataset",
    schema_version: int | None = None,
) -> DataFrame:
    _, schema_definition = resolve_schema_definition(dataset_name, dataset, schema_version)
    source = raw_path(config, dataset)
    if schema_definition["format"] == "csv":
        reader = spark.read
        for key, value in schema_definition.get("read_options", {}).items():
            reader = reader.option(key, value)
        return reader.csv(source)
    if schema_definition["format"] == "parquet":
        return spark.read.parquet(source)
    raise ValueError(f"Unsupported format: {schema_definition['format']}")


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def write_delta(
    df: DataFrame,
    path: str,
    partitions: list[str] | None = None,
    mode: str = "overwrite",
    merge_schema: bool = False,
) -> None:
    writer = df.write.format("delta").mode(mode)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    elif merge_schema:
        writer = writer.option("mergeSchema", "true")
    if partitions:
        writer = writer.partitionBy(*partitions)
    writer.save(path)


def timed(label: str, fn: Callable[[], T]) -> T:
    started = time.perf_counter()
    result = fn()
    print(f"{label}: {time.perf_counter() - started:.2f}s")
    return result


def directory_size(path: Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def file_count(path: Path) -> int:
    return sum(1 for item in path.rglob("*") if item.is_file())
