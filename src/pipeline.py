from __future__ import annotations

import argparse

from src.common import create_spark, load_config
from src.data_aggregation.aggregate_tables import build_aggregate_tables
from src.data_analysis.benchmark import run_benchmark
from src.data_cleaning.normal_tables import build_normal_tables
from src.data_consumption.raw_tables import build_raw_tables
from src.data_integration.integrated_tables import build_integrated_tables


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("step", choices=["raw", "normal", "integrated", "aggregate", "benchmark", "all"])
    args = parser.parse_args()

    config = load_config()
    spark = create_spark()

    steps = {
        "raw": build_raw_tables,
        "normal": build_normal_tables,
        "integrated": build_integrated_tables,
        "aggregate": build_aggregate_tables,
        "benchmark": run_benchmark,
    }

    for step, build in steps.items():
        if args.step in {step, "all"}:
            build(spark, config)


if __name__ == "__main__":
    main()
