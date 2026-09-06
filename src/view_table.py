from __future__ import annotations

import argparse
from pathlib import Path

from src.common import create_spark, load_config, read_delta, table_path


DEFAULT_LIMIT = 10


def resolve_table_path(config: dict, table: str, layer: str) -> str:
    datasets = config.get("datasets", {})
    dataset = datasets.get(table)
    layer_key = f"{layer}_table"

    if dataset and dataset.get(layer_key):
        return table_path(config, dataset[layer_key])

    path = Path(table)
    if path.is_absolute() or path.exists():
        return str(path)

    return table_path(config, table)


def parse_columns(value: str | None) -> list[str] | None:
    if value is None:
        return None

    columns = [column.strip() for column in value.split(",") if column.strip()]
    return columns or None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read and preview a Delta table with Spark.")
    parser.add_argument(
        "table",
        help=(
            "Dataset name from config, lakehouse relative path such as raw/yellow_taxi_trips, "
            "or an absolute Delta table path."
        ),
    )
    parser.add_argument(
        "--layer",
        default="raw",
        choices=["raw", "normal", "integrated", "aggregate", "benchmark"],
        help="Layer used when table is a dataset name from config. Default: raw.",
    )
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Number of rows to display.")
    parser.add_argument("--columns", help="Comma-separated column list to display.")
    parser.add_argument("--no-count", action="store_true", help="Skip row count for large tables.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config()
    delta_path = resolve_table_path(config, args.table, args.layer)
    columns = parse_columns(args.columns)

    spark = create_spark()
    try:
        df = read_delta(spark, delta_path)
        if columns:
            missing_columns = [column for column in columns if column not in df.columns]
            if missing_columns:
                raise ValueError(f"Columns not found in table: {', '.join(missing_columns)}")
            df = df.select(*columns)

        print(f"Delta table: {delta_path}")
        print("Schema:")
        df.printSchema()
        if not args.no_count:
            print(f"Rows: {df.count()}")
        df.show(args.limit, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
