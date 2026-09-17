"""Independent entry point for Week 2 analytical workloads."""
from __future__ import annotations

import argparse
from pathlib import Path

from src.common import create_spark, load_config
from src.data_analysis.analytical_queries import run_analytical_queries
from src.data_analysis.data_products import PRODUCT_BUILDERS, build_data_products
from src.data_analysis.data_product_report import generate_data_product_report
from src.data_analysis.query_library import ANALYTICAL_QUERIES


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/config.yaml"))
    commands = parser.add_subparsers(dest="command", required=True)

    query_parser = commands.add_parser("queries", help="Run reusable Spark SQL queries")
    query_parser.add_argument("--query", choices=list(ANALYTICAL_QUERIES))
    query_parser.add_argument("--save", action="store_true")

    product_parser = commands.add_parser("products", help="Refresh reusable Delta products")
    product_parser.add_argument("--product", choices=list(PRODUCT_BUILDERS))

    report_parser = commands.add_parser("report", help="Generate the standalone data-product HTML report")
    report_parser.add_argument("--output", type=Path)

    args = parser.parse_args()
    config = load_config(args.config)
    spark = create_spark()
    try:
        if args.command == "queries":
            run_analytical_queries(spark, config, args.query, args.save)
        elif args.command == "products":
            build_data_products(spark, config, args.product)
        else:
            output = generate_data_product_report(spark, config, args.output)
            print(f"Generated data product report: {output}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
