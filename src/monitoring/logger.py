from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.common import table_path


PIPELINE_RUN_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("parent_run_id", StringType(), True),
        StructField("release_id", StringType(), True),
        StructField("operation_type", StringType(), False),
        StructField("pipeline_step", StringType(), True),
        StructField("dataset_name", StringType(), True),
        StructField("operation_name", StringType(), True),
        StructField("target_table", StringType(), True),
        StructField("status", StringType(), False),
        StructField("started_at", TimestampType(), False),
        StructField("finished_at", TimestampType(), False),
        StructField("duration_seconds", DoubleType(), False),
        StructField("processed_records", LongType(), True),
        StructField("inserted_records", LongType(), True),
        StructField("duplicate_records", LongType(), True),
        StructField("rejected_records", LongType(), True),
        StructField("validation_failures", LongType(), True),
        StructField("schema_version", IntegerType(), True),
        StructField("error_type", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("details_json", StringType(), True),
    ]
)


SCHEMA_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("release_id", StringType(), True),
        StructField("dataset_name", StringType(), False),
        StructField("old_schema_version", IntegerType(), True),
        StructField("new_schema_version", IntegerType(), False),
        StructField("change_type", StringType(), False),
        StructField("added_columns_json", StringType(), False),
        StructField("removed_columns_json", StringType(), False),
        StructField("changed_types_json", StringType(), False),
        StructField("compatible", BooleanType(), False),
        StructField("detected_at", TimestampType(), False),
    ]
)


def monitoring_enabled(config: dict) -> bool:
    """Return whether operational monitoring is enabled in configuration."""
    return bool(config.get("monitoring", {}).get("enabled", False))


def utc_now() -> datetime:
    """Return a timezone-normalized UTC timestamp compatible with Spark TimestampType."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value
        return value.astimezone(timezone.utc).replace(tzinfo=None)

    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def _json(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, sort_keys=True, default=str)


def _sum_present(*values: Any) -> int | None:
    present = [int(value) for value in values if value is not None]
    return sum(present) if present else None

def _rows_to_dataframe(
    spark: SparkSession,
    rows: list[dict],
    schema: StructType,
):
    """
    Build a tiny DataFrame without serializing
    Python Row objects through a Python worker.

    Monitoring batches contain only a few rows,
    so JVM-side literals are simpler and more
    reliable for local Windows execution.
    """

    dataframes = []

    for row in rows:
        columns = []

        for field in schema.fields:
            value = row.get(field.name)

            columns.append(
                F.lit(value)
                .cast(field.dataType)
                .alias(field.name)
            )

        dataframe = (
            spark.range(1)
            .select(*columns)
        )

        dataframes.append(dataframe)

    result = dataframes[0]

    for dataframe in dataframes[1:]:
        result = result.unionByName(
            dataframe
        )

    return result

def _write_rows(
    spark: SparkSession,
    config: dict,
    rows: list[dict],
    *,
    config_key: str,
    schema: StructType,
    transaction_id: str,
) -> None:
    """Append one small monitoring batch to a Delta table idempotently."""
    if not rows or not monitoring_enabled(config):
        return

    destination = table_path(
        config,
        config["monitoring"][config_key],
    )

    dataframe = _rows_to_dataframe(
        spark,
        rows,
        schema,
    )

    (
        dataframe.write
        .format("delta")
        .mode("append")
        .option("txnAppId", transaction_id)
        .option("txnVersion", 0)
        .save(destination)
    )


def record_operation(
    spark: SparkSession,
    config: dict,
    *,
    operation_type: str,
    pipeline_step: str | None,
    status: str,
    started_at: datetime,
    finished_at: datetime,
    duration_seconds: float,
    run_id: str | None = None,
    parent_run_id: str | None = None,
    release_id: str | None = None,
    dataset_name: str | None = None,
    operation_name: str | None = None,
    target_table: str | None = None,
    processed_records: int | None = None,
    inserted_records: int | None = None,
    duplicate_records: int | None = None,
    rejected_records: int | None = None,
    validation_failures: int | None = None,
    schema_version: int | None = None,
    error: Exception | None = None,
    details: dict | None = None,
) -> str:
    """Write one semantic platform operation to monitoring/pipeline_runs."""
    run_id = run_id or str(uuid.uuid4())
    row = {
        "run_id": run_id,
        "parent_run_id": parent_run_id,
        "release_id": release_id,
        "operation_type": operation_type,
        "pipeline_step": pipeline_step,
        "dataset_name": dataset_name,
        "operation_name": operation_name,
        "target_table": target_table,
        "status": status.upper(),
        "started_at": _parse_timestamp(started_at),
        "finished_at": _parse_timestamp(finished_at),
        "duration_seconds": float(duration_seconds),
        "processed_records": processed_records,
        "inserted_records": inserted_records,
        "duplicate_records": duplicate_records,
        "rejected_records": rejected_records,
        "validation_failures": validation_failures,
        "schema_version": schema_version,
        "error_type": type(error).__name__ if error else None,
        "error_message": str(error) if error else None,
        "details_json": _json(details),
    }
    _write_rows(
        spark,
        config,
        [row],
        config_key="pipeline_runs_table",
        schema=PIPELINE_RUN_SCHEMA,
        transaction_id=f"monitoring-operation:{run_id}",
    )
    return run_id


def safe_record_operation(*args, **kwargs) -> str | None:
    """Best-effort wrapper: monitoring failure must not corrupt a successful data update."""
    try:
        return record_operation(*args, **kwargs)
    except Exception as error:  # monitoring must not change business-table correctness
        print(f"Monitoring warning: failed to record operation: {error}", flush=True)
        return None


def record_incremental_attempt(
    spark: SparkSession,
    config: dict,
    manifest: dict,
    state: dict,
    *,
    attempt_id: str,
    started_at: datetime,
    finished_at: datetime,
    duration_seconds: float,
    status: str,
    error: Exception | None = None,
    include_dataset_rows: bool = True,
) -> None:
    """Write one release-attempt row plus dataset-level child rows in one Delta append."""
    if not monitoring_enabled(config):
        return

    release_id = (
            manifest.get("release_id")
            or state.get("release_id")
    )

    dataset_items = (
        list(state.get("datasets", {}).items())
        if include_dataset_rows
        else []
    )

    # Only datasets actually processed during this attempt
    # contribute to release-level metrics.
    #
    # A resumed release may contain datasets that were already
    # completed during a previous attempt. Their state still
    # contains old counts, but the current attempt marks them
    # as SKIPPED. Those historical counts must not be counted
    # again in the current release attempt totals.
    metric_items = [
        (dataset_name, progress)
        for dataset_name, progress in dataset_items
        if str(progress.get("attempt_status", "")).upper() != "SKIPPED"
    ]

    processed_total = _sum_present(
        *[
            progress.get("input_records")
            for _, progress in metric_items
        ]
    )

    inserted_total = _sum_present(
        *[
            progress.get("inserted_raw")
            for _, progress in metric_items
        ]
    )

    duplicate_total = _sum_present(
        *[
            progress.get("ignored_duplicates")
            for _, progress in metric_items
        ]
    )

    rejected_total = _sum_present(
        *[
            _sum_present(
                progress.get("consumption_rejected"),
                progress.get("cleaning_rejected"),
            )
            for _, progress in metric_items
        ]
    )

    validation_total = _sum_present(
        *[
            progress.get("validation_failures")
            for _, progress in metric_items
        ]
    )

    parent_row = {
        "run_id": attempt_id,
        "parent_run_id": None,
        "release_id": release_id,
        "operation_type": "incremental_release",
        "pipeline_step": "updates",
        "dataset_name": None,
        "operation_name": release_id,
        "target_table": None,
        "status": status.upper(),
        "started_at": _parse_timestamp(started_at),
        "finished_at": _parse_timestamp(finished_at),
        "duration_seconds": float(duration_seconds),
        "processed_records": processed_total,
        "inserted_records": inserted_total,
        "duplicate_records": duplicate_total,
        "rejected_records": rejected_total,
        "validation_failures": validation_total,
        "schema_version": None,
        "error_type": type(error).__name__ if error else None,
        "error_message": str(error) if error else None,
        "details_json": _json(
            {
                "integrated_changed": state.get("integrated_changed"),
                "affected_months": state.get("affected_months", []),
                "affected_dates": state.get("affected_dates", []),
                "products_done": state.get("products_done", []),
                "products_skipped": state.get("products_skipped", []),
            }
        ),
    }

    rows = [parent_row]
    overall_status = status.upper()

    for dataset_name, progress in dataset_items:
        dataset_config = config["datasets"].get(dataset_name, {})
        attempt_status = progress.get("attempt_status")

        if attempt_status:
            dataset_status = attempt_status.upper()
        else:
            completed = progress.get("normal_done") is True

            dataset_status = (
                "SUCCESS"
                if completed
                else (
                    "FAILED"
                    if overall_status == "FAILED"
                    else "PARTIAL"
                )
            )

        consumption_rejected = progress.get("consumption_rejected")
        cleaning_rejected = progress.get("cleaning_rejected")
        rejected = _sum_present(consumption_rejected, cleaning_rejected)
        validation_failures = progress.get("validation_failures")
        if validation_failures is None:
            validation_failures = _sum_present(
                progress.get("consumption_validation_failures"),
                progress.get("cleaning_validation_failures"),
            )

        child_error = error if dataset_status == "FAILED" else None
        is_skipped = dataset_status == "SKIPPED"
        child_run_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{attempt_id}:{dataset_name}"))
        rows.append(
            {
                "run_id": child_run_id,
                "parent_run_id": attempt_id,
                "release_id": release_id,
                "operation_type": "dataset_update",
                "pipeline_step": "updates",
                "dataset_name": dataset_name,
                "operation_name": "incremental_update",
                "target_table": dataset_config.get("normal_table"),
                "status": dataset_status,
                "started_at": _parse_timestamp(progress.get("started_at")) or _parse_timestamp(started_at),
                "finished_at": _parse_timestamp(progress.get("finished_at")) or _parse_timestamp(finished_at),
                "duration_seconds": float(progress.get("processing_seconds", 0.0)),
                "processed_records": (
                    None
                    if is_skipped
                    else progress.get("input_records")
                ),
                "inserted_records": (
                    None
                    if is_skipped
                    else progress.get("inserted_raw")
                ),
                "duplicate_records": (
                    None
                    if is_skipped
                    else progress.get("ignored_duplicates")
                ),
                "rejected_records": (
                    None
                    if is_skipped
                    else rejected
                ),
                "validation_failures": (
                    None
                    if is_skipped
                    else validation_failures
                ),
                "schema_version": progress.get("schema_version"),
                "error_type": type(child_error).__name__ if child_error else None,
                "error_message": str(child_error) if child_error else None,
                "details_json": _json(
                    {
                        "raw_table": dataset_config.get("raw_table"),
                        "normal_table": dataset_config.get("normal_table"),
                        "normal_changed": progress.get("normal_changed"),
                        "consumption_rejected": consumption_rejected,
                        "cleaning_rejected": cleaning_rejected,
                        "consumption_validation_failures": progress.get(
                            "consumption_validation_failures"
                        ),
                        "cleaning_validation_failures": progress.get(
                            "cleaning_validation_failures"
                        ),
                    }
                ),
            }
        )

    _write_rows(
        spark,
        config,
        rows,
        config_key="pipeline_runs_table",
        schema=PIPELINE_RUN_SCHEMA,
        transaction_id=f"monitoring-incremental:{attempt_id}",
    )


def safe_record_incremental_attempt(*args, **kwargs) -> None:
    try:
        record_incremental_attempt(*args, **kwargs)
    except Exception as error:
        print(f"Monitoring warning: failed to record incremental attempt: {error}", flush=True)


def _previous_schema_version(dataset_config: dict, new_version: int) -> int | None:
    versions = sorted(int(version) for version in dataset_config["schema_versions"])
    previous = [version for version in versions if version < new_version]
    return max(previous) if previous else None


def _schema_diff(dataset_config: dict, old_version: int, new_version: int) -> dict:
    old_definition = dataset_config["schema_versions"][str(old_version)]
    new_definition = dataset_config["schema_versions"][str(new_version)]

    old_columns = set(old_definition["expected_columns"])
    new_columns = set(new_definition["expected_columns"])
    added = sorted(new_columns - old_columns)
    removed = sorted(old_columns - new_columns)

    changed_types = {}
    old_types = old_definition["column_types"]
    new_types = new_definition["column_types"]
    for column in sorted(old_columns & new_columns):
        if old_types[column] != new_types[column]:
            changed_types[column] = {
                "old": old_types[column],
                "new": new_types[column],
            }

    change_types = []
    if added:
        change_types.append("ADD_COLUMN")
    if removed:
        change_types.append("REMOVE_COLUMN")
    if changed_types:
        change_types.append("TYPE_CHANGE")

    return {
        "change_type": "+".join(change_types) if change_types else "NO_CHANGE",
        "added": added,
        "removed": removed,
        "changed_types": changed_types,
        "compatible": bool(added) and not removed and not changed_types,
    }


def record_schema_events(
    spark: SparkSession,
    config: dict,
    manifest: dict,
    *,
    run_id: str,
) -> None:
    """Persist schema-version transitions once per dataset/version pair."""
    if not monitoring_enabled(config):
        return

    destination = table_path(config, config["monitoring"]["schema_events_table"])
    existing: set[tuple[str, int]] = set()
    if DeltaTable.isDeltaTable(spark, destination):
        existing = {
            (row.dataset_name, int(row.new_schema_version))
            for row in (
                spark.read.format("delta")
                .load(destination)
                .select("dataset_name", "new_schema_version")
                .distinct()
                .collect()
            )
        }

    rows = []
    release_id = manifest.get("release_id")
    detected_at = utc_now()

    for entry in manifest.get("datasets", []):
        dataset_name = entry["dataset"]
        new_version = int(entry["schema_version"])
        if (dataset_name, new_version) in existing:
            continue

        dataset_config = config["datasets"][dataset_name]
        old_version = _previous_schema_version(dataset_config, new_version)
        if old_version is None:
            continue

        difference = _schema_diff(dataset_config, old_version, new_version)
        if difference["change_type"] == "NO_CHANGE":
            continue

        rows.append(
            {
                "event_id": str(
                    uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        f"schema:{dataset_name}:{old_version}:{new_version}",
                    )
                ),
                "run_id": run_id,
                "release_id": release_id,
                "dataset_name": dataset_name,
                "old_schema_version": old_version,
                "new_schema_version": new_version,
                "change_type": difference["change_type"],
                "added_columns_json": _json(difference["added"]) or "[]",
                "removed_columns_json": _json(difference["removed"]) or "[]",
                "changed_types_json": _json(difference["changed_types"]) or "{}",
                "compatible": difference["compatible"],
                "detected_at": detected_at,
            }
        )

    _write_rows(
        spark,
        config,
        rows,
        config_key="schema_events_table",
        schema=SCHEMA_EVENT_SCHEMA,
        transaction_id=f"monitoring-schema:{run_id}",
    )


def safe_record_schema_events(*args, **kwargs) -> None:
    try:
        record_schema_events(*args, **kwargs)
    except Exception as error:
        print(f"Monitoring warning: failed to record schema events: {error}", flush=True)
