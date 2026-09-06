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
T = TypeVar("T")


def load_config(path: Path = CONFIG_PATH) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


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


def read_source(spark: SparkSession, config: dict, dataset: dict) -> DataFrame:
    source = raw_path(config, dataset)
    if dataset["format"] == "csv":
        return spark.read.option("header", True).option("inferSchema", True).csv(source)
    if dataset["format"] == "parquet":
        return spark.read.parquet(source)
    raise ValueError(f"Unsupported format: {dataset['format']}")


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def write_delta(df: DataFrame, path: str, partitions: list[str] | None = None, mode: str = "overwrite") -> None:
    writer = df.write.format("delta").mode(mode)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
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
