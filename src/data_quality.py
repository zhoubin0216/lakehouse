"""Reusable row validation, quarantine storage, and validation reporting."""
from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Callable, Iterable

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType
from pyspark.sql.window import Window

from src.common import table_path, write_delta


RulePredicate = Callable[[DataFrame], Column]


class SchemaContractError(ValueError):
    """An unsupported source schema that requires a contract/version change."""

    def __init__(self, message: str, *, details: dict | None = None):
        super().__init__(message)
        self.details = details or {}


@dataclass(frozen=True)
class ValidationRule:
    """One composable validation rule whose predicate is true for invalid rows."""

    rule_id: str
    message: str
    category: str
    predicate: RulePredicate

    def condition(self, df: DataFrame) -> Column:
        return self.predicate(df)


class ValidationRegistry:
    """Registry for generic (``*``) and dataset-specific validation rules."""

    def __init__(self) -> None:
        self._rules: dict[str, list[ValidationRule]] = {}

    def register(self, dataset_name: str, *rules: ValidationRule) -> None:
        self._rules.setdefault(dataset_name, []).extend(rules)

    def rules_for(self, dataset_name: str) -> list[ValidationRule]:
        return [
            *self._rules.get("*", []),
            *self._rules.get(dataset_name, []),
        ]

    def classify(
        self,
        df: DataFrame,
        dataset_name: str,
        stage: str,
        extra_rules: Iterable[ValidationRule] = (),
    ) -> tuple[DataFrame, DataFrame]:
        return classify_records(
            df,
            stage,
            [*self.rules_for(dataset_name), *extra_rules],
        )


def required_rule(column: str, message: str | None = None) -> ValidationRule:
    """Reject a null value in a required column."""
    label = column.lstrip("_").replace("_", " ")
    return ValidationRule(
        rule_id=f"required.{column.lstrip('_')}",
        message=message or f"{label} is required",
        category="incomplete",
        predicate=lambda _df, name=column: F.col(name).isNull(),
    )


def non_empty_text_rule(column: str, message: str | None = None) -> ValidationRule:
    """Reject null, empty, or whitespace-only text."""
    label = column.replace("_", " ")
    return ValidationRule(
        rule_id=f"required.{column}",
        message=message or f"{label} is required",
        category="incomplete",
        predicate=lambda _df, name=column: (
            F.col(name).isNull() | (F.trim(F.col(name)) == "")
        ),
    )


def range_rule(
    column: str,
    minimum: float,
    maximum: float,
    message: str | None = None,
) -> ValidationRule:
    """Reject non-null numeric values outside an inclusive range."""
    return ValidationRule(
        rule_id=f"range.{column}",
        message=message or f"{column} must be between {minimum} and {maximum}",
        category="invalid_value",
        predicate=lambda _df, name=column, low=minimum, high=maximum: (
            F.col(name).isNotNull() & ~F.col(name).between(low, high)
        ),
    )


def minimum_rule(
    column: str,
    minimum: float,
    message: str | None = None,
) -> ValidationRule:
    """Reject non-null numeric values below a minimum."""
    return ValidationRule(
        rule_id=f"minimum.{column}",
        message=message or f"{column} must be at least {minimum}",
        category="invalid_value",
        predicate=lambda _df, name=column, low=minimum: (
            F.col(name).isNotNull() & (F.col(name) < low)
        ),
    )


def predicate_rule(
    rule_id: str,
    message: str,
    category: str,
    predicate: RulePredicate,
) -> ValidationRule:
    """Create a dataset-specific rule without modifying the core framework."""
    return ValidationRule(rule_id, message, category, predicate)


def _legacy_rule(reason: str, condition: Column) -> ValidationRule:
    rule_id = re.sub(r"[^a-z0-9]+", ".", reason.lower()).strip(".")
    return ValidationRule(rule_id, reason, "dataset_specific", lambda _df: condition)


def _normalize_rules(
    rules: Iterable[ValidationRule | tuple[str, Column]],
) -> list[ValidationRule]:
    return [
        rule if isinstance(rule, ValidationRule) else _legacy_rule(*rule)
        for rule in rules
    ]


def classify_records(
    df: DataFrame,
    stage: str,
    rejection_rules: Iterable[ValidationRule | tuple[str, Column]],
) -> tuple[DataFrame, DataFrame]:
    """Split records and attach structured rule IDs, categories, and reasons."""
    rules = _normalize_rules(rejection_rules)
    if rules:
        rule_ids = F.array_compact(
            F.array(*[F.when(rule.condition(df), F.lit(rule.rule_id)) for rule in rules])
        )
        categories = F.array_compact(
            F.array(*[F.when(rule.condition(df), F.lit(rule.category)) for rule in rules])
        )
        reasons = F.array_compact(
            F.array(*[F.when(rule.condition(df), F.lit(rule.message)) for rule in rules])
        )
    else:
        empty = F.array().cast("array<string>")
        rule_ids = categories = reasons = empty

    evaluated = (
        df.withColumn("_validation_rule_ids", rule_ids)
        .withColumn("_validation_categories", categories)
        .withColumn("_rejection_reasons", reasons)
    )
    accepted = evaluated.filter(F.size("_rejection_reasons") == 0).drop(
        "_validation_rule_ids",
        "_validation_categories",
        "_rejection_reasons",
    )
    rejected = (
        evaluated.filter(F.size("_rejection_reasons") > 0)
        .withColumn("_rejection_stage", F.lit(stage))
        .withColumn("_rejected_at", F.current_timestamp())
    )
    return accepted, rejected


def reject_all(df: DataFrame, stage: str, rule: ValidationRule) -> DataFrame:
    """Mark every row in a DataFrame as rejected by one rule."""
    always = ValidationRule(
        rule.rule_id,
        rule.message,
        rule.category,
        lambda _df: F.lit(True),
    )
    return classify_records(df, stage, [always])[1]


def combine_rejections(*frames: DataFrame) -> DataFrame:
    """Union rejection frames whose source schemas may differ."""
    if not frames:
        raise ValueError("At least one rejection DataFrame is required")
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame, allowMissingColumns=True)
    return result


def split_duplicate_records(
    df: DataFrame,
    key_columns: list[str],
    *,
    stage: str = "deduplication",
) -> tuple[DataFrame, DataFrame]:
    """Keep one row per exact key and quarantine every additional copy."""
    missing = sorted(set(key_columns) - set(df.columns))
    if missing:
        raise ValueError(f"Duplicate check is missing key columns: {missing}")
    rank = Window.partitionBy(*key_columns).orderBy(F.monotonically_increasing_id())
    ranked = df.withColumn("_duplicate_rank", F.row_number().over(rank))
    accepted = ranked.filter("_duplicate_rank = 1").drop("_duplicate_rank")
    duplicates = reject_all(
        ranked.filter("_duplicate_rank > 1").drop("_duplicate_rank"),
        stage,
        ValidationRule(
            "duplicate.exact_record",
            "duplicate record",
            "duplicate",
            lambda _df: F.lit(True),
        ),
    )
    return accepted, duplicates


@dataclass(frozen=True)
class ReferenceRule:
    rule_id: str
    source_column: str
    reference_column: str
    message: str


def classify_reference_records(
    df: DataFrame,
    reference_df: DataFrame,
    rules: Iterable[ReferenceRule],
    *,
    stage: str = "reference",
) -> tuple[DataFrame, DataFrame]:
    """Validate foreign-key-like fields using distributed left joins."""
    evaluated = df
    validation_rules: list[ValidationRule] = []
    markers: list[str] = []
    for index, rule in enumerate(rules):
        if rule.source_column not in evaluated.columns:
            raise ValueError(f"Missing reference source column: {rule.source_column}")
        if rule.reference_column not in reference_df.columns:
            raise ValueError(f"Missing reference target column: {rule.reference_column}")
        reference_key = f"_reference_key_{index}"
        marker = f"_reference_match_{index}"
        keys = (
            reference_df.select(F.col(rule.reference_column).alias(reference_key))
            .filter(F.col(reference_key).isNotNull())
            .distinct()
            .withColumn(marker, F.lit(True))
        )
        evaluated = evaluated.join(
            F.broadcast(keys),
            evaluated[rule.source_column] == keys[reference_key],
            "left",
        ).drop(reference_key)
        markers.append(marker)
        validation_rules.append(
            ValidationRule(
                rule.rule_id,
                rule.message,
                "missing_reference",
                lambda _df, source=rule.source_column, match=marker: (
                    F.col(source).isNotNull() & F.col(match).isNull()
                ),
            )
        )
    accepted, rejected = classify_records(evaluated, stage, validation_rules)
    return accepted.drop(*markers), rejected.drop(*markers)


def rejected_table_path(config: dict, stage: str, dataset_name: str) -> str:
    root = config["data_quality"]["rejected_table_root"].strip("/")
    return table_path(config, f"{root}/{stage}/{dataset_name}")


def validation_summary_path(config: dict) -> str:
    relative = config["data_quality"].get(
        "validation_summary_table", "validation/rule_summary"
    )
    return table_path(config, relative)


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


VALIDATION_SUMMARY_SCHEMA = StructType(
    [
        StructField("dataset_name", StringType(), False),
        StructField("stage", StringType(), False),
        StructField("rule_id", StringType(), False),
        StructField("category", StringType(), False),
        StructField("reason", StringType(), False),
        StructField("failed_records", LongType(), False),
    ]
)


def build_validation_report(spark: SparkSession, config: dict) -> DataFrame:
    """Aggregate current quarantine snapshots by dataset, stage, and rule."""
    frames: list[DataFrame] = []
    stages = ("consumption", "cleaning", "deduplication", "reference")
    for dataset_name in config["datasets"]:
        for stage in stages:
            path = rejected_table_path(config, stage, dataset_name)
            if not DeltaTable.isDeltaTable(spark, path):
                continue
            rejected = spark.read.format("delta").load(path)
            if "_rejection_reasons" not in rejected.columns:
                continue
            reasons = F.col("_rejection_reasons")
            rule_ids = (
                F.col("_validation_rule_ids")
                if "_validation_rule_ids" in rejected.columns
                else F.transform(reasons, lambda reason: reason)
            )
            categories = (
                F.col("_validation_categories")
                if "_validation_categories" in rejected.columns
                else F.transform(reasons, lambda _reason: F.lit("legacy"))
            )
            failures = rejected.select(
                F.lit(dataset_name).alias("dataset_name"),
                F.lit(stage).alias("stage"),
                F.explode(
                    F.arrays_zip(
                        rule_ids.alias("rule_ids"),
                        categories.alias("categories"),
                        reasons.alias("reasons"),
                    )
                ).alias("failure"),
            ).select(
                "dataset_name",
                "stage",
                F.col("failure.rule_ids").alias("rule_id"),
                F.col("failure.categories").alias("category"),
                F.col("failure.reasons").alias("reason"),
            )
            frames.append(failures)

    if frames:
        failures = frames[0]
        for frame in frames[1:]:
            failures = failures.unionByName(frame)
        summary = (
            failures.groupBy("dataset_name", "stage", "rule_id", "category", "reason")
            .count()
            .withColumnRenamed("count", "failed_records")
            .orderBy(F.desc("failed_records"), "dataset_name", "stage", "rule_id")
        )
    else:
        summary = spark.createDataFrame([], VALIDATION_SUMMARY_SCHEMA)

    write_delta(summary, validation_summary_path(config))
    return summary


def main() -> None:
    from src.common import create_spark, load_config

    spark = create_spark()
    try:
        build_validation_report(spark, load_config()).show(200, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
