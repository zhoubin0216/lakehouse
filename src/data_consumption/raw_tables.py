import re

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common import resolve_schema_definition, table_path, write_delta
from src.data_quality import (
    SchemaContractError,
    classify_records,
    split_duplicate_records,
    write_rejected_records,
)
from src.data_consumption.metadata import (
    mark_ingestion_failure,
    mark_ingestion_success,
    save_ingestion_metadata,
    start_ingestion_run,
)
from src.data_consumption.registry import (
    add_checksum,
    add_schema_version,
    discover_source_files,
    find_files_to_consume,
    get_file_state,
    load_source_file_registry,
    save_source_file_registry,
)


def build_raw_tables(spark: SparkSession, config: dict) -> dict:
    """Consume all configured datasets and return an ingestion summary."""
    dataset_results = []
    for dataset_name, dataset_config in config["datasets"].items():
        dataset_results.append(
            consume_dataset(spark, dataset_name, dataset_config, config)
        )

    summary = {
        "datasets": dataset_results,
        "consumed_files": sum(result["consumed_files"] for result in dataset_results),
        "accepted_records": sum(result["accepted_records"] for result in dataset_results),
        "rejected_records": sum(result["rejected_records"] for result in dataset_results),
    }
    summary["has_new_data"] = summary["accepted_records"] > 0
    return summary


def consume_dataset(
    spark: SparkSession,
    dataset_name: str,
    dataset_config: dict,
    config: dict,
) -> dict:
    """Consume new or changed source data and return dataset-level metrics."""
    schema_version, schema_definition = resolve_schema_definition(
        dataset_name, dataset_config, dataset_config.get("source_schema_version")
    )
    run = start_ingestion_run(dataset_name, schema_version)
    try:
        source_files = discover_source_files(dataset_name, dataset_config, schema_definition, config)
        registry = load_source_file_registry(dataset_name, config)
        files_to_consume = source_files if is_sample_run(config) else find_files_to_consume(source_files, registry)

        if not files_to_consume:
            save_ingestion_metadata(mark_ingestion_success(run, processed_records=0), config)
            print(f"{dataset_name}: no new or changed source files")
            return ingestion_result(dataset_name, schema_version)

        raw_df, rejected_df = load_source_files(
            spark,
            files_to_consume,
            dataset_name,
            schema_version,
            schema_definition,
            config,
            run["run_id"],
        )
        duplicate_df = None
        if should_deduplicate_records(config):
            raw_df, duplicate_df = split_duplicate_records(
                raw_df,
                ["_record_hash"],
            )
        processed_records = write_raw_delta(raw_df, dataset_config, config)
        write_mode = "overwrite" if is_sample_run(config) else "append"
        consumption_rejected = write_rejected_records(
            rejected_df,
            config,
            stage="consumption",
            dataset_name=dataset_name,
            mode=write_mode,
        )
        duplicate_rejected = (
            write_rejected_records(
                duplicate_df,
                config,
                stage="deduplication",
                dataset_name=dataset_name,
                mode=write_mode,
            )
            if duplicate_df is not None
            else 0
        )
        rejected_records = consumption_rejected + duplicate_rejected

        if is_sample_run(config):
            print(f"{dataset_name}: sample run, source registry not updated")
        else:
            checksum_max_bytes = config["data_consumption"].get("checksum_max_bytes", 0)
            consumed_states = [
                add_schema_version(
                    add_checksum(get_file_state(path), checksum_max_bytes),
                    schema_version,
                )
                for path in files_to_consume
            ]
            save_source_file_registry(dataset_name, consumed_states, config)
        save_ingestion_metadata(
            mark_ingestion_success(run, processed_records, rejected_records),
            config,
        )
        print(
            f"{dataset_name}: consumed {processed_records} rows and rejected "
            f"{rejected_records} rows from {len(files_to_consume)} file(s)"
        )
        return ingestion_result(
            dataset_name,
            schema_version,
            consumed_files=len(files_to_consume),
            accepted_records=processed_records,
            rejected_records=rejected_records,
        )
    except Exception as error:
        save_ingestion_metadata(mark_ingestion_failure(run, error), config)
        raise


def ingestion_result(
    dataset_name: str,
    schema_version: int,
    consumed_files: int = 0,
    accepted_records: int = 0,
    rejected_records: int = 0,
) -> dict:
    """Build the stable result contract consumed by pipeline orchestration."""
    return {
        "dataset_name": dataset_name,
        "schema_version": schema_version,
        "consumed_files": consumed_files,
        "accepted_records": accepted_records,
        "rejected_records": rejected_records,
        "has_new_data": accepted_records > 0,
    }


def load_source_files(
    spark: SparkSession,
    source_files: list,
    dataset_name: str,
    schema_version: int,
    schema_definition: dict,
    config: dict,
    ingestion_run_id: str,
) -> tuple[DataFrame, DataFrame]:
    """Load CSV or Parquet files selected for consumption."""
    sample_limit = config.get("data_consumption", {}).get("sample_limit")
    accepted_dfs = []
    rejected_dfs = []

    for source_file in source_files:
        df = load_one_source_file(spark, source_file, schema_definition)
        validate_source_columns(df, schema_definition)
        accepted, rejected = validate_and_cast_source_types(df, schema_definition)
        source_state = get_file_state(source_file)
        accepted = add_lineage_columns(
            make_delta_safe_columns(accepted, schema_definition),
            dataset_name,
            schema_version,
            ingestion_run_id,
            source_state,
        )
        rejected = add_lineage_columns(
            make_delta_safe_columns(rejected, schema_definition),
            dataset_name,
            schema_version,
            ingestion_run_id,
            source_state,
        )
        accepted_dfs.append(accepted)
        rejected_dfs.append(rejected)

    if not accepted_dfs:
        raise ValueError("No source files to load")

    accepted_result = union_dataframes(accepted_dfs)
    rejected_result = union_dataframes(rejected_dfs)

    if sample_limit:
        limit = int(sample_limit)
        return accepted_result.limit(limit), rejected_result.limit(limit)
    return accepted_result, rejected_result


def union_dataframes(dataframes: list[DataFrame]) -> DataFrame:
    result = dataframes[0]
    for dataframe in dataframes[1:]:
        result = result.unionByName(dataframe, allowMissingColumns=True)
    return result


def load_one_source_file(spark: SparkSession, source_file, schema_definition: dict) -> DataFrame:
    """Load one source file so lineage columns can be injected accurately."""
    path = str(source_file)
    if schema_definition["format"] == "csv":
        reader = spark.read
        for key, value in schema_definition.get("read_options", {}).items():
            reader = reader.option(key, value)
        return reader.csv(path)
    if schema_definition["format"] == "parquet":
        return spark.read.parquet(path)
    raise ValueError(f"Unsupported format: {schema_definition['format']}")


def add_lineage_columns(
    df: DataFrame,
    dataset_name: str,
    schema_version: int,
    ingestion_run_id: str,
    source_state: dict,
) -> DataFrame:
    """Add source, schema-contract, ingestion-run, and record lineage fields."""
    with_source = (
        df.withColumn("_dataset_name", F.lit(dataset_name))
        .withColumn("_schema_version", F.lit(schema_version).cast("int"))
        .withColumn("_source_file", F.lit(source_state["path"]))
        .withColumn("_source_file_size", F.lit(source_state["size_bytes"]))
        .withColumn("_source_modified_time", F.lit(source_state["modified_time_ns"]))
        .withColumn("_ingestion_run_id", F.lit(ingestion_run_id))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
    )
    data_columns = [column for column in with_source.columns if not column.startswith("_")]
    return with_source.withColumn(
        "_record_hash",
        F.sha2(F.concat_ws("||", *[F.col(column).cast("string") for column in data_columns]), 256),
    )


def deduplicate_records(df: DataFrame) -> DataFrame:
    """Use _record_hash or business keys to avoid duplicate raw records."""
    accepted, _ = split_duplicate_records(df, ["_record_hash"])
    return accepted


def write_raw_delta(df, dataset_config: dict, config: dict) -> int:
    """Append raw records to the configured raw Delta table and return processed row count."""
    row_count = df.count()
    write_mode = "overwrite" if is_sample_run(config) else config.get("data_consumption", {}).get("write_mode", "append")
    write_delta(
        df,
        table_path(config, dataset_config["raw_table"]),
        mode=write_mode,
        merge_schema=True,
    )
    return row_count


def is_sample_run(config: dict) -> bool:
    return bool(config.get("data_consumption", {}).get("sample_limit"))


def should_deduplicate_records(config: dict) -> bool:
    return bool(config.get("data_consumption", {}).get("deduplicate_records", False))


def validate_source_columns(df: DataFrame, schema_definition: dict) -> None:
    """Require source columns to match the selected schema contract exactly."""
    expected_columns = set(schema_definition["expected_columns"])
    actual_columns = set(df.columns)
    missing_columns = sorted(expected_columns - actual_columns)
    unexpected_columns = sorted(actual_columns - expected_columns)
    if missing_columns or unexpected_columns:
        raise SchemaContractError(
            "Source columns do not match the schema contract; "
            f"missing={missing_columns}, unexpected={unexpected_columns}",
            details={
                "change_type": "UNSUPPORTED_COLUMNS",
                "missing_columns": missing_columns,
                "unexpected_columns": unexpected_columns,
            },
        )


def validate_and_cast_source_types(
    df: DataFrame,
    schema_definition: dict,
) -> tuple[DataFrame, DataFrame]:
    """Strictly validate Parquet types or split invalid CSV values into rejects."""
    if schema_definition["format"] == "parquet":
        validate_parquet_column_types(df, schema_definition)
        return df, empty_type_rejections(df)

    cast_columns = []
    rejection_rules = []
    for column in schema_definition["expected_columns"]:
        expected_type = schema_definition["column_types"][column]
        source = F.col(column)
        cast_value = (
            source.cast("string")
            if expected_type == "string"
            else try_cast(column, expected_type)
        )
        has_value = source.isNotNull() & (F.trim(source.cast("string")) != "")
        rejection_rules.append(
            (
                f"{column}: expected {expected_type}",
                has_value & cast_value.isNull(),
            )
        )
        cast_columns.append(cast_value.alias(column))

    accepted, rejected = classify_records(
        df,
        stage="consumption",
        rejection_rules=rejection_rules,
    )
    return accepted.select(*cast_columns), rejected


def validate_parquet_column_types(df: DataFrame, schema_definition: dict) -> None:
    expected_types = schema_definition["column_types"]
    actual_types = {field.name: field.dataType.simpleString() for field in df.schema.fields}
    mismatches = {
        column: {"expected": expected_types[column], "actual": actual_types[column]}
        for column in schema_definition["expected_columns"]
        if actual_types[column] != expected_types[column]
    }
    if mismatches:
        raise SchemaContractError(
            f"Parquet column types do not match the schema contract: {mismatches}",
            details={
                "change_type": "UNSUPPORTED_TYPE_CHANGE",
                "type_mismatches": mismatches,
            },
        )


def try_cast(column: str, expected_type: str):
    escaped_column = column.replace("`", "``")
    return F.expr(f"try_cast(`{escaped_column}` AS {expected_type})")


def empty_type_rejections(df: DataFrame) -> DataFrame:
    return (
        df.limit(0)
        .withColumn("_rejection_reasons", F.array().cast("array<string>"))
        .withColumn("_rejection_stage", F.lit("consumption"))
        .withColumn("_rejected_at", F.current_timestamp())
    )


def make_delta_safe_columns(df: DataFrame, schema_definition: dict) -> DataFrame:
    """Rename source columns to Delta-safe names before writing raw tables."""
    mapping = schema_definition.get("columns", {})
    used_names = set()

    for source in df.columns:
        target = (
            source
            if source.startswith("_")
            else mapping.get(source, to_snake_case(source))
        )
        target = unique_name(target, used_names)
        used_names.add(target)
        if source != target:
            df = df.withColumnRenamed(source, target)

    return df


def to_snake_case(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "column"


def unique_name(name: str, used_names: set[str]) -> str:
    if name not in used_names:
        return name

    index = 2
    while f"{name}_{index}" in used_names:
        index += 1
    return f"{name}_{index}"
