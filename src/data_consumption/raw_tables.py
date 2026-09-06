import re

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common import table_path, write_delta
from src.data_consumption.metadata import (
    mark_ingestion_failure,
    mark_ingestion_success,
    save_ingestion_metadata,
    start_ingestion_run,
)
from src.data_consumption.registry import (
    add_checksum,
    discover_source_files,
    find_files_to_consume,
    get_file_state,
    load_source_file_registry,
    save_source_file_registry,
)


def build_raw_tables(spark: SparkSession, config: dict) -> None:
    """Consume all configured datasets into raw Delta tables."""
    for dataset_name, dataset_config in config["datasets"].items():
        consume_dataset(spark, dataset_name, dataset_config, config)


def consume_dataset(spark: SparkSession, dataset_name: str, dataset_config: dict, config: dict) -> None:
    """Consume only new or changed source data for one dataset."""
    run = start_ingestion_run(dataset_name)
    try:
        source_files = discover_source_files(dataset_name, dataset_config, config)
        registry = load_source_file_registry(dataset_name, config)
        files_to_consume = source_files if is_sample_run(config) else find_files_to_consume(source_files, registry)

        if not files_to_consume:
            save_ingestion_metadata(mark_ingestion_success(run, processed_records=0), config)
            print(f"{dataset_name}: no new or changed source files")
            return

        raw_df = load_source_files(spark, files_to_consume, dataset_name, dataset_config, config, run["run_id"])
        if should_deduplicate_records(config):
            raw_df = deduplicate_records(raw_df)
        processed_records = write_raw_delta(raw_df, dataset_config, config)

        if is_sample_run(config):
            print(f"{dataset_name}: sample run, source registry not updated")
        else:
            checksum_max_bytes = config["data_consumption"].get("checksum_max_bytes", 0)
            consumed_states = [add_checksum(get_file_state(path), checksum_max_bytes) for path in files_to_consume]
            save_source_file_registry(dataset_name, consumed_states, config)
        save_ingestion_metadata(mark_ingestion_success(run, processed_records), config)
        print(f"{dataset_name}: consumed {processed_records} rows from {len(files_to_consume)} file(s)")
    except Exception as error:
        save_ingestion_metadata(mark_ingestion_failure(run, error), config)
        raise


def load_source_files(
    spark: SparkSession,
    source_files: list,
    dataset_name: str,
    dataset_config: dict,
    config: dict,
    ingestion_run_id: str,
) -> DataFrame:
    """Load CSV or Parquet files selected for consumption."""
    sample_limit = config.get("data_consumption", {}).get("sample_limit")
    dfs = []

    for source_file in source_files:
        df = load_one_source_file(spark, source_file, dataset_config)
        validate_expected_columns(df, dataset_config)
        df = make_delta_safe_columns(df, dataset_config)
        df = add_lineage_columns(df, dataset_name, ingestion_run_id, get_file_state(source_file))
        dfs.append(df)

    if not dfs:
        raise ValueError("No source files to load")

    result = dfs[0]
    for df in dfs[1:]:
        result = result.unionByName(df, allowMissingColumns=True)

    return result.limit(int(sample_limit)) if sample_limit else result


def load_one_source_file(spark: SparkSession, source_file, dataset_config: dict) -> DataFrame:
    """Load one source file so lineage columns can be injected accurately."""
    path = str(source_file)
    if dataset_config["format"] == "csv":
        reader = spark.read
        for key, value in dataset_config.get("read_options", {}).items():
            reader = reader.option(key, value)
        return reader.csv(path)
    if dataset_config["format"] == "parquet":
        return spark.read.parquet(path)
    raise ValueError(f"Unsupported format: {dataset_config['format']}")


def add_lineage_columns(df: DataFrame, dataset_name: str, ingestion_run_id: str, source_state: dict) -> DataFrame:
    """Add _source_file, _ingestion_run_id, _ingestion_timestamp, and file-state fields."""
    with_source = (
        df.withColumn("_dataset_name", F.lit(dataset_name))
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
    return df.dropDuplicates(["_record_hash"])


def write_raw_delta(df, dataset_config: dict, config: dict) -> int:
    """Append raw records to the configured raw Delta table and return processed row count."""
    row_count = df.count()
    write_mode = "overwrite" if is_sample_run(config) else config.get("data_consumption", {}).get("write_mode", "append")
    write_delta(df, table_path(config, dataset_config["raw_table"]), mode=write_mode)
    return row_count


def is_sample_run(config: dict) -> bool:
    return bool(config.get("data_consumption", {}).get("sample_limit"))


def should_deduplicate_records(config: dict) -> bool:
    return bool(config.get("data_consumption", {}).get("deduplicate_records", False))


def validate_expected_columns(df: DataFrame, dataset_config: dict) -> None:
    expected_columns = dataset_config.get("expected_columns", [])
    missing_columns = sorted(set(expected_columns) - set(df.columns))
    if missing_columns:
        raise ValueError(f"Missing expected columns: {missing_columns}")


def make_delta_safe_columns(df: DataFrame, dataset_config: dict) -> DataFrame:
    """Rename source columns to Delta-safe names before writing raw tables."""
    mapping = dataset_config.get("columns", {})
    used_names = set()

    for source in df.columns:
        target = mapping.get(source, to_snake_case(source))
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
