"""Metadata helpers for reusable analytical data products."""
from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.common import read_delta, table_path, write_delta


CATALOG_SCHEMA = StructType(
    [
        StructField("product_name", StringType(), False),
        StructField("table_path", StringType(), False),
        StructField("description", StringType(), False),
        StructField("intended_users", StringType(), False),
        StructField("grain", StringType(), False),
        StructField("materialization_reason", StringType(), False),
        StructField("source_table", StringType(), False),
        StructField("source_delta_version", LongType(), False),
        StructField("source_schema_versions", StringType(), False),
        StructField("product_schema_version", IntegerType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("refreshed_at", TimestampType(), False),
        StructField("row_count", LongType(), False),
        StructField("storage_bytes", LongType(), False),
        StructField("refresh_duration_seconds", DoubleType(), False),
        StructField("partitions", ArrayType(StringType(), False), False),
    ]
)


def delta_snapshot(spark: SparkSession, path: str) -> dict:
    """Return the current Delta version and active storage metrics."""
    table = DeltaTable.forPath(spark, path)
    history = table.history(1).select("version", "timestamp").first()
    detail = table.detail().select("sizeInBytes", "numFiles").first()
    return {
        "version": int(history.version),
        "commit_timestamp": history.timestamp,
        "size_bytes": int(detail.sizeInBytes),
        "active_files": int(detail.numFiles),
    }


def load_catalog_records(spark: SparkSession, config: dict) -> dict[str, dict]:
    """Load the small product catalog into a name-keyed mapping."""
    path = product_catalog_path(config)
    if not Path(path).exists() or not DeltaTable.isDeltaTable(spark, path):
        return {}
    return {
        row["product_name"]: row.asDict(recursive=True)
        for row in read_delta(spark, path).collect()
    }


def attach_product_metadata(
    dataframe: DataFrame,
    *,
    product_name: str,
    product_schema_version: int,
    source_table: str,
    source_delta_version: int,
    source_schema_versions: dict,
    created_at: datetime,
    refreshed_at: datetime,
) -> DataFrame:
    """Add the metadata required by Task 4 to every product row."""
    return (
        dataframe.withColumn("_product_name", F.lit(product_name))
        .withColumn("_product_schema_version", F.lit(product_schema_version).cast("int"))
        .withColumn("_source_table", F.lit(source_table))
        .withColumn("_source_delta_version", F.lit(source_delta_version).cast("long"))
        .withColumn(
            "_source_schema_versions",
            F.lit(json.dumps(source_schema_versions, sort_keys=True)),
        )
        .withColumn("_created_at", F.lit(created_at).cast("timestamp"))
        .withColumn("_refreshed_at", F.lit(refreshed_at).cast("timestamp"))
    )


def write_product_catalog(
    spark: SparkSession,
    config: dict,
    refreshed_records: list[dict],
    existing_records: dict[str, dict] | None = None,
) -> None:
    """Replace refreshed catalog rows while preserving all other products."""
    records = dict(existing_records or load_catalog_records(spark, config))
    records.update({record["product_name"]: record for record in refreshed_records})
    dataframe = spark.createDataFrame(list(records.values()), schema=CATALOG_SCHEMA)
    write_delta(dataframe, product_catalog_path(config))


def product_catalog_path(config: dict) -> str:
    relative = config["data_analysis"]["products"]["catalog_table"]
    return table_path(config, relative)
