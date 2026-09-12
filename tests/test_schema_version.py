from pathlib import Path
from datetime import date

import pytest
from pyspark.sql import SparkSession

from src.common import read_delta, write_delta
from src.data_quality import rejected_table_path, write_rejected_records
from src.data_consumption.metadata import mark_ingestion_success, start_ingestion_run
from src.data_consumption.raw_tables import (
    add_lineage_columns,
    validate_and_cast_source_types,
    validate_parquet_column_types,
    validate_source_columns,
)
from src.data_consumption.registry import add_schema_version, find_files_to_consume, get_file_state


def test_schema_version_is_preserved_in_run_metadata() -> None:
    run = start_ingestion_run("weather_hourly", schema_version=2)

    completed = mark_ingestion_success(run, processed_records=10)

    assert completed["schema_version"] == 2


def test_schema_version_is_added_to_raw_rows(spark: SparkSession) -> None:
    raw = spark.createDataFrame([(1,)], ["record_id"])
    source_state = {
        "path": "data/raw/example.csv",
        "size_bytes": 10,
        "modified_time_ns": 20,
    }

    row = add_lineage_columns(
        raw,
        dataset_name="example",
        schema_version=3,
        ingestion_run_id="run-1",
        source_state=source_state,
    ).first()

    assert row._schema_version == 3
    assert row._dataset_name == "example"
    assert row._ingestion_run_id == "run-1"


def test_schema_version_bump_does_not_reconsume_unchanged_file(tmp_path: Path) -> None:
    source_file = tmp_path / "source.csv"
    source_file.write_text("id\n1\n", encoding="utf-8")
    previous_state = add_schema_version(get_file_state(source_file), schema_version=1)
    registry = {"dataset": "example", "files": [previous_state]}

    assert find_files_to_consume([source_file], registry) == []


def test_delta_schema_can_evolve_when_version_column_is_added(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    table_path = str(tmp_path / "raw_table")
    old_record = spark.createDataFrame([(1,)], ["record_id"])
    new_record = spark.createDataFrame([(2, 2)], ["record_id", "_schema_version"])

    write_delta(old_record, table_path)
    write_delta(new_record, table_path, mode="append", merge_schema=True)

    rows = read_delta(spark, table_path).orderBy("record_id").collect()
    assert rows[0]._schema_version is None
    assert rows[1]._schema_version == 2


def test_csv_values_are_strictly_cast_or_rejected(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    source = spark.createDataFrame(
        [("1", "2.5", "2024-01-02"), ("bad", "2.5", "2024-01-02")],
        ["id", "amount", "event_date"],
    )
    schema_definition = {
        "format": "csv",
        "expected_columns": ["id", "amount", "event_date"],
        "column_types": {
            "id": "int",
            "amount": "double",
            "event_date": "date",
        },
    }

    validate_source_columns(source, schema_definition)
    accepted, rejected = validate_and_cast_source_types(source, schema_definition)

    accepted_row = accepted.first()
    rejected_row = rejected.first()
    assert accepted_row.id == 1
    assert accepted_row.amount == 2.5
    assert accepted_row.event_date == date(2024, 1, 2)
    assert rejected_row._rejection_stage == "consumption"
    assert rejected_row._rejection_reasons == ["id: expected int"]

    config = {
        "paths": {"lakehouse": str(tmp_path)},
        "data_quality": {"rejected_table_root": "rejected"},
    }
    assert write_rejected_records(
        rejected,
        config,
        stage="consumption",
        dataset_name="example",
        mode="overwrite",
    ) == 1
    stored = read_delta(
        spark,
        rejected_table_path(config, "consumption", "example"),
    ).first()
    assert stored.id == "bad"


def test_parquet_physical_type_mismatch_is_rejected(spark: SparkSession) -> None:
    source = spark.createDataFrame([(1,)], ["id"])
    schema_definition = {
        "format": "parquet",
        "expected_columns": ["id"],
        "column_types": {"id": "int"},
    }

    with pytest.raises(ValueError, match="Parquet column types do not match"):
        validate_parquet_column_types(source, schema_definition)
