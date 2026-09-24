from copy import deepcopy

from pyspark.sql import SparkSession

from src.common import load_config, read_delta
from src.data_quality import (
    ValidationRegistry,
    build_validation_report,
    classify_records,
    minimum_rule,
    required_rule,
    split_duplicate_records,
    validation_summary_path,
    write_rejected_records,
)


def test_registry_combines_generic_and_dataset_specific_rules(
    spark: SparkSession,
) -> None:
    records = spark.createDataFrame(
        [("ok", 5.0), (None, 5.0), ("negative", -1.0)],
        "record_id string, amount double",
    )
    registry = ValidationRegistry()
    registry.register("*", required_rule("record_id"))
    registry.register("payments", minimum_rule("amount", 0))

    accepted, rejected = registry.classify(records, "payments", "cleaning")

    assert [row.record_id for row in accepted.collect()] == ["ok"]
    failures = {
        row.record_id: (row._validation_rule_ids, row._validation_categories)
        for row in rejected.collect()
    }
    assert failures[None] == (["required.record_id"], ["incomplete"])
    assert failures["negative"] == (["minimum.amount"], ["invalid_value"])


def test_duplicate_records_are_quarantined_instead_of_silently_dropped(
    spark: SparkSession,
) -> None:
    records = spark.createDataFrame(
        [("a", 1), ("a", 1), ("b", 2)],
        "record_hash string, value int",
    )

    accepted, rejected = split_duplicate_records(records, ["record_hash"])

    assert accepted.count() == 2
    duplicate = rejected.first()
    assert duplicate.record_hash == "a"
    assert duplicate._validation_rule_ids == ["duplicate.exact_record"]
    assert duplicate._validation_categories == ["duplicate"]


def test_validation_report_aggregates_quarantine_by_rule(
    spark: SparkSession,
    tmp_path,
) -> None:
    config = deepcopy(load_config())
    config["paths"]["lakehouse"] = str(tmp_path / "lakehouse")
    source = spark.createDataFrame(
        [(None,), (None,), ("valid",)],
        "record_id string",
    )
    _, rejected = classify_records(
        source,
        "cleaning",
        [required_rule("record_id")],
    )
    write_rejected_records(
        rejected,
        config,
        stage="cleaning",
        dataset_name="weather_hourly",
        mode="overwrite",
    )

    report = build_validation_report(spark, config)

    row = report.first()
    assert row.dataset_name == "weather_hourly"
    assert row.stage == "cleaning"
    assert row.rule_id == "required.record_id"
    assert row.category == "incomplete"
    assert row.failed_records == 2
    assert read_delta(spark, validation_summary_path(config)).count() == 1
