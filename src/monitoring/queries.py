from __future__ import annotations

import argparse
from pathlib import Path

from delta.tables import DeltaTable

from src.common import create_spark, load_config, read_delta, table_path


SQL_DIR = Path(__file__).with_name("sql")
QUERY_FILES = {
    "validation_failures_by_dataset": "validation_failures_by_dataset.sql",
    "processing_time_by_dataset": "processing_time_by_dataset.sql",
    "rejected_records_by_run": "rejected_records_by_run.sql",
    "processing_time_trend": "processing_time_trend.sql",
    "schema_evolution_history": "schema_evolution_history.sql",
}


def load_monitoring_sql(query_name: str) -> str:
    if query_name not in QUERY_FILES:
        raise ValueError(f"Unknown monitoring query: {query_name}")
    return (SQL_DIR / QUERY_FILES[query_name]).read_text(encoding="utf-8")


def register_monitoring_views(spark, config: dict) -> None:
    """Expose monitoring Delta tables as Spark SQL temporary views."""
    monitoring = config["monitoring"]

    runs_path = table_path(config, monitoring["pipeline_runs_table"])
    if not DeltaTable.isDeltaTable(spark, runs_path):
        raise FileNotFoundError(
            f"Monitoring table does not exist yet: {runs_path}. "
            "Run at least one monitored pipeline execution first."
        )
    read_delta(spark, runs_path).createOrReplaceTempView("monitoring_pipeline_runs")

    events_path = table_path(config, monitoring["schema_events_table"])
    if DeltaTable.isDeltaTable(spark, events_path):
        read_delta(spark, events_path).createOrReplaceTempView("monitoring_schema_events")


def run_monitoring_queries(spark, config: dict, query_name: str | None = None) -> dict:
    """Run one or all Task 3 operational monitoring SQL queries."""
    register_monitoring_views(spark, config)
    names = [query_name] if query_name else list(QUERY_FILES)
    results = {}

    for name in names:
        if name == "schema_evolution_history" and not spark.catalog.tableExists(
            "monitoring_schema_events"
        ):
            print("Query: schema_evolution_history (no schema events table yet)")
            continue

        print(f"\nQuery: {name}")
        result = spark.sql(load_monitoring_sql(name))
        result.show(100, truncate=False)
        results[name] = result

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Query Week 3 operational monitoring tables")
    parser.add_argument("--query", choices=list(QUERY_FILES))
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--config", type=Path, default=Path("configs/config.yaml"))
    args = parser.parse_args()

    if args.list:
        print("\n".join(QUERY_FILES))
        return

    config = load_config(args.config)
    spark = create_spark()
    try:
        run_monitoring_queries(spark, config, args.query)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
