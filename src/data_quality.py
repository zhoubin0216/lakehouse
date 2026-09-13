from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from src.common import table_path, write_delta


def classify_records(
    df: DataFrame,
    stage: str,
    rejection_rules: list[tuple[str, Column]],
) -> tuple[DataFrame, DataFrame]:
    """Split records into accepted and rejected DataFrames with explicit reasons."""
    reasons = F.array_compact(
        F.array(
            *[
                F.when(condition, F.lit(reason))
                for reason, condition in rejection_rules
            ]
        )
    )
    evaluated = df.withColumn("_rejection_reasons", reasons)
    accepted = evaluated.filter(F.size("_rejection_reasons") == 0).drop(
        "_rejection_reasons"
    )
    rejected = (
        evaluated.filter(F.size("_rejection_reasons") > 0)
        .withColumn("_rejection_stage", F.lit(stage))
        .withColumn("_rejected_at", F.current_timestamp())
    )
    return accepted, rejected


def rejected_table_path(config: dict, stage: str, dataset_name: str) -> str:
    root = config["data_quality"]["rejected_table_root"].strip("/")
    return table_path(config, f"{root}/{stage}/{dataset_name}")


def write_rejected_records(
    df: DataFrame,
    config: dict,
    stage: str,
    dataset_name: str,
    mode: str,
) -> int:
    """Write rejected records and return their count."""
    rejected_count = df.count()
    if rejected_count or mode == "overwrite":
        write_delta(
            df,
            rejected_table_path(config, stage, dataset_name),
            mode=mode,
            merge_schema=mode == "append",
        )
    return rejected_count
