from __future__ import annotations

import time
from pathlib import Path
from typing import Callable, TypeVar

import yaml
from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession


CONFIG_PATH = Path("configs/config.yaml")
T = TypeVar("T")


def load_config(path: Path = CONFIG_PATH) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def create_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("week1-lakehouse")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def raw_path(config: dict, dataset: dict) -> str:
    return str(Path(config["paths"]["raw"]) / dataset["source"])


def table_path(config: dict, relative_path: str) -> str:
    return str(Path(config["paths"]["lakehouse"]) / relative_path)


def read_source(spark: SparkSession, config: dict, dataset: dict) -> DataFrame:
    source = raw_path(config, dataset)
    if dataset["format"] == "csv":
        return spark.read.option("header", True).option("inferSchema", True).csv(source)
    if dataset["format"] == "parquet":
        return spark.read.parquet(source)
    raise ValueError(f"Unsupported format: {dataset['format']}")


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def write_delta(df: DataFrame, path: str, partitions: list[str] | None = None) -> None:
    writer = df.write.format("delta").mode("overwrite")
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
