from __future__ import annotations

import argparse

from src.common import create_spark, load_config
from src.data_aggregation.aggregate_tables import build_aggregate_tables
from src.data_cleaning.normal_tables import build_normal_tables
from src.data_consumption.raw_tables import build_raw_tables
from src.data_integration.integrated_tables import build_integrated_tables

def run_incremental_pipeline(spark, config: dict) -> dict:
    """Consume sources and rebuild derived tables only when new valid rows arrive."""
    ingestion = build_raw_tables(spark, config)
    if not ingestion["has_new_data"]:
        print("No new accepted raw records; skipping normal, integrated, and aggregate steps")
        return ingestion

    print(
        f"New data detected: {ingestion['accepted_records']} accepted row(s) "
        f"from {ingestion['consumed_files']} file(s)"
    )
    build_normal_tables(spark, config)
    build_integrated_tables(spark, config)
    build_aggregate_tables(spark, config)
    return ingestion


def run_pipeline_step(spark, config: dict, step: str):
    """Run one manual step or the conditional incremental pipeline."""
    if step == "all":
        return run_incremental_pipeline(spark, config)

    steps = {
        "raw": build_raw_tables,
        "normal": build_normal_tables,
        "integrated": build_integrated_tables,
        "aggregate": build_aggregate_tables,

    }
    return steps[step](spark, config)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a lakehouse step or the conditional incremental pipeline."
    )
    parser.add_argument(
        "step",
        choices=["raw", "normal", "integrated", "aggregate", "all"],
    )
    args = parser.parse_args()

    config = load_config()
    spark = create_spark()
    try:
        run_pipeline_step(spark, config, args.step)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
