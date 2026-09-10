from __future__ import annotations

import csv
import json
import shutil
import statistics
import time
from pathlib import Path
from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import (
    directory_size,
    file_count,
    read_delta,
    table_path,
    write_delta,
)


# =========================================================
# Benchmark input
# =========================================================

def prepare_benchmark_data(
    integrated: DataFrame,
) -> DataFrame:
    """
    Keep taxi-trip attributes required for the benchmark.

    Weather and air-quality columns are excluded because Task 6
    benchmarks storage strategies for taxi-trip analytical data.
    """

    columns = [
        "trip_id",
        "vendor_id",
        "pickup_timestamp",
        "dropoff_timestamp",
        "passenger_count",
        "trip_distance",
        "pickup_location_id",
        "dropoff_location_id",
        "payment_type",
        "fare_amount",
        "total_amount",
        "trip_duration_seconds",
        "pickup_date",
        "pickup_year",
        "pickup_month",
        "pickup_borough",
    ]

    missing = [
        column
        for column in columns
        if column not in integrated.columns
    ]

    if missing:
        raise ValueError(
            f"Benchmark source is missing columns: {missing}"
        )

    return integrated.select(*columns)


# =========================================================
# Required Task 6 queries
# =========================================================

def query_trips_per_borough(
    df: DataFrame,
) -> DataFrame:
    """Number of taxi trips per pickup borough."""
    return (
        df.groupBy("pickup_borough")
        .agg(
            F.count("*").alias("trip_count"),
        )
    )


def query_avg_trip_duration_per_day(
    df: DataFrame,
) -> DataFrame:
    """Average trip duration per pickup date."""
    return (
        df.groupBy("pickup_date")
        .agg(
            F.avg("trip_duration_seconds")
            .alias("avg_trip_duration_seconds"),
        )
    )


def query_avg_fare_per_borough(
    df: DataFrame,
) -> DataFrame:
    """Average fare amount per pickup borough."""
    return (
        df.groupBy("pickup_borough")
        .agg(
            F.avg("fare_amount")
            .alias("avg_fare_amount"),
        )
    )


# =========================================================
# File / directory helpers
# =========================================================

def clear_directory(path: str) -> None:
    """
    Delete an old benchmark table before rewriting it.

    This prevents old Delta versions from affecting storage-size
    and file-count measurements.
    """
    output = Path(path)

    if output.exists():
        shutil.rmtree(output)


def count_parquet_files(path: str) -> int:
    """Count physical Parquet data files."""
    return sum(
        1
        for item in Path(path).rglob("*.parquet")
        if item.is_file()
    )


# =========================================================
# Write benchmark
# =========================================================

def measure_write(
    df: DataFrame,
    output_path: str,
    partitions: list[str] | None,
    runs: int = 3,
) -> dict:
    """
    Write the same Delta table multiple times.

    Each run starts from a clean output directory.
    The final run is kept on disk and is later used for query benchmarks.
    """

    timings = []

    for run_number in range(1, runs + 1):
        clear_directory(output_path)

        started = time.perf_counter()

        write_delta(
            df,
            output_path,
            partitions=partitions,
        )

        elapsed = time.perf_counter() - started
        timings.append(elapsed)

        print(
            f"  Write run {run_number}/{runs}: "
            f"{elapsed:.3f}s"
        )

    return {
        "median": statistics.median(timings),
        "mean": statistics.mean(timings),
        "minimum": min(timings),
        "maximum": max(timings),
        "runs": timings,
    }


# =========================================================
# Query benchmark
# =========================================================

def measure_query(
    spark: SparkSession,
    df: DataFrame,
    query_fn: Callable[[DataFrame], DataFrame],
    warmup_runs: int,
    measured_runs: int,
) -> dict:
    """
    Measure Spark query latency.

    collect() is required because Spark uses lazy evaluation.
    """

    # Warm-up runs are not included in statistics.
    for _ in range(warmup_runs):
        query_fn(df).collect()

    timings = []

    for run_number in range(1, measured_runs + 1):
        spark.catalog.clearCache()

        started = time.perf_counter()

        # Trigger actual Spark execution.
        query_fn(df).collect()

        elapsed = time.perf_counter() - started
        timings.append(elapsed)

        print(
            f"    Query run {run_number}/{measured_runs}: "
            f"{elapsed:.4f}s"
        )

    return {
        "median": statistics.median(timings),
        "mean": statistics.mean(timings),
        "minimum": min(timings),
        "maximum": max(timings),
        "runs": timings,
    }


# =========================================================
# Save benchmark results
# =========================================================

def save_results(
    output_path: str,
    results: list[dict],
) -> None:
    """Save benchmark results to CSV."""

    destination = Path(output_path)

    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    if not results:
        return

    with destination.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as handle:

        writer = csv.DictWriter(
            handle,
            fieldnames=list(results[0].keys()),
        )

        writer.writeheader()
        writer.writerows(results)


# =========================================================
# Main benchmark
# =========================================================

def run_benchmark(
    spark: SparkSession,
    config: dict,
) -> None:
    """Compare Delta storage strategies."""

    benchmark_config = config["benchmark"]
    integration_config = config["data_integration"]

    # -----------------------------------------------------
    # Load benchmark source
    # -----------------------------------------------------

    integrated_path = table_path(
        config,
        integration_config["integrated_taxi_trips_table"],
    )

    integrated = read_delta(
        spark,
        integrated_path,
    )

    benchmark_df = prepare_benchmark_data(
        integrated,
    )

    # -----------------------------------------------------
    # Storage strategies
    # -----------------------------------------------------

    strategies = {
        "unpartitioned": {
            "table":
                benchmark_config["unpartitioned_table"],
            "partitions": None,
        },

        "pickup_borough": {
            "table":
                benchmark_config["borough_partitioned_table"],
            "partitions": ["pickup_borough"],
        },

        "pickup_month": {
            "table":
                benchmark_config["month_partitioned_table"],
            "partitions": ["pickup_month"],
        },

        "pickup_date": {
            "table":
                benchmark_config["date_partitioned_table"],
            "partitions": ["pickup_date"],
        },
    }
    # Only the three queries required by Task 6.
    queries = {
        "trips_per_borough":
            query_trips_per_borough,

        "avg_trip_duration_per_day":
            query_avg_trip_duration_per_day,

        "avg_fare_per_borough":
            query_avg_fare_per_borough,
    }

    write_runs = benchmark_config.get(
        "write_runs",
        3,
    )

    warmup_runs = benchmark_config.get(
        "warmup_runs",
        1,
    )

    measured_runs = benchmark_config.get(
        "measured_runs",
        5,
    )

    results = []

    # -----------------------------------------------------
    # Benchmark each strategy
    # -----------------------------------------------------

    for strategy_name, strategy in strategies.items():

        print()
        print("=" * 65)
        print(f"Benchmark strategy: {strategy_name}")
        print(
            "Partition columns: "
            f"{strategy['partitions'] or 'none'}"
        )
        print("=" * 65)

        output_path = table_path(
            config,
            strategy["table"],
        )

        # =================================================
        # 1. Write benchmark
        # =================================================

        print("\nWrite benchmark:")

        write_metrics = measure_write(
            benchmark_df,
            output_path,
            strategy["partitions"],
            runs=write_runs,
        )

        print(
            f"  Write median: "
            f"{write_metrics['median']:.3f}s"
        )

        print(
            f"  Write mean: "
            f"{write_metrics['mean']:.3f}s"
        )

        # =================================================
        # 2. Physical storage metrics
        # =================================================

        output_directory = Path(output_path)

        storage_bytes = directory_size(
            output_directory
        )

        total_files = file_count(
            output_directory
        )

        parquet_files = count_parquet_files(
            output_path
        )

        print(
            f"\nStorage size: "
            f"{storage_bytes / 1024 / 1024:.2f} MB"
        )

        print(
            f"Total files: {total_files}"
        )

        print(
            f"Parquet data files: {parquet_files}"
        )

        # =================================================
        # 3. Read table back for query benchmark
        # =================================================

        spark.catalog.clearCache()

        stored_df = read_delta(
            spark,
            output_path,
        )

        # Basic row for CSV output.
        row = {
            "strategy":
                strategy_name,

            "partition_columns":
                ",".join(
                    strategy["partitions"] or []
                ),

            # ----- write metrics -----

            "write_median_seconds":
                round(
                    write_metrics["median"],
                    4,
                ),

            "write_mean_seconds":
                round(
                    write_metrics["mean"],
                    4,
                ),

            "write_min_seconds":
                round(
                    write_metrics["minimum"],
                    4,
                ),

            "write_max_seconds":
                round(
                    write_metrics["maximum"],
                    4,
                ),

            # Save every individual write time.
            "write_runs_seconds":
                json.dumps(
                    [
                        round(value, 4)
                        for value
                        in write_metrics["runs"]
                    ]
                ),

            # ----- physical layout metrics -----

            "storage_size_mb":
                round(
                    storage_bytes
                    / 1024
                    / 1024,
                    2,
                ),

            "total_file_count":
                total_files,

            "parquet_file_count":
                parquet_files,
        }

        # =================================================
        # 4. Required query benchmarks
        # =================================================

        for query_name, query_fn in queries.items():

            print(
                f"\nQuery: {query_name}"
            )

            metrics = measure_query(
                spark,
                stored_df,
                query_fn,
                warmup_runs,
                measured_runs,
            )

            # Median
            row[
                f"{query_name}_median_seconds"
            ] = round(
                metrics["median"],
                4,
            )

            # Mean
            row[
                f"{query_name}_mean_seconds"
            ] = round(
                metrics["mean"],
                4,
            )

            # Minimum
            row[
                f"{query_name}_min_seconds"
            ] = round(
                metrics["minimum"],
                4,
            )

            # Maximum
            row[
                f"{query_name}_max_seconds"
            ] = round(
                metrics["maximum"],
                4,
            )

            # IMPORTANT:
            # Save every measured run in the CSV.
            row[
                f"{query_name}_runs_seconds"
            ] = json.dumps(
                [
                    round(value, 4)
                    for value
                    in metrics["runs"]
                ]
            )

            print(
                f"  Median: "
                f"{metrics['median']:.4f}s"
            )

            print(
                f"  Mean: "
                f"{metrics['mean']:.4f}s"
            )

        results.append(row)

    # -----------------------------------------------------
    # Save CSV
    # -----------------------------------------------------

    result_path = table_path(
        config,
        benchmark_config["results_file"],
    )

    save_results(
        result_path,
        results,
    )

    print()
    print("=" * 65)
    print(
        f"Benchmark results saved to: "
        f"{result_path}"
    )
    print("=" * 65)