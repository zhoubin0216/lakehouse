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
# Random file-count control strategies
# =========================================================

def add_random_bucket(
    df: DataFrame,
    column_name: str,
    bucket_count: int,
    seed: int,
) -> DataFrame:
    """
    Add a deterministic pseudo-random bucket column.

    These buckets are used as control strategies so that we can compare
    semantic partitioning with a random layout that has the same target
    number of Parquet data files. Using a hash instead of rand() keeps the
    assignment stable across repeated benchmark runs.
    """
    if bucket_count <= 0:
        raise ValueError("bucket_count must be positive")

    return df.withColumn(
        column_name,
        F.pmod(
            F.xxhash64(
                F.col("trip_id"),
                F.lit(seed),
            ),
            F.lit(bucket_count),
        ).cast("int"),
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
    repartition_count: int | None = None,
    repartition_columns: list[str] | None = None,
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

        write_df = df

        # Random control strategies explicitly repartition by their bucket
        # column so that one bucket is written by one Spark partition. This
        # makes the number of Parquet data files directly comparable with
        # the target file count from the corresponding semantic strategy.
        if repartition_count is not None:
            columns = repartition_columns or []
            if not columns:
                raise ValueError(
                    "repartition_columns are required when "
                    "repartition_count is set"
                )

            write_df = write_df.repartition(
                repartition_count,
                *[F.col(column) for column in columns],
            )

        write_delta(
            write_df,
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
    # Random file-count controls
    # -----------------------------------------------------

    # These target counts come from the final benchmark results of the
    # corresponding semantic partitioning strategies. They are configurable
    # so the controls can easily be updated if the dataset changes.
    random_borough_file_count = benchmark_config.get(
        "random_borough_file_count",
        128,
    )
    random_month_file_count = benchmark_config.get(
        "random_month_file_count",
        20,
    )
    random_date_file_count = benchmark_config.get(
        "random_date_file_count",
        608,
    )

    random_borough_column = "random_bucket_128"
    random_month_column = "random_bucket_20"
    random_date_column = "random_bucket_608"

    random_borough_df = add_random_bucket(
        benchmark_df,
        random_borough_column,
        random_borough_file_count,
        seed=101,
    )
    random_month_df = add_random_bucket(
        benchmark_df,
        random_month_column,
        random_month_file_count,
        seed=202,
    )
    random_date_df = add_random_bucket(
        benchmark_df,
        random_date_column,
        random_date_file_count,
        seed=303,
    )

    # -----------------------------------------------------
    # Storage strategies
    # -----------------------------------------------------

    strategies = {
        "unpartitioned": {
            "table":
                benchmark_config["unpartitioned_table"],
            "partitions": None,
            "dataframe": benchmark_df,
            "repartition_count": None,
            "repartition_columns": None,
        },

        "pickup_borough": {
            "table":
                benchmark_config["borough_partitioned_table"],
            "partitions": ["pickup_borough"],
            "dataframe": benchmark_df,
            "repartition_count": None,
            "repartition_columns": None,
        },

        "pickup_month": {
            "table":
                benchmark_config["month_partitioned_table"],
            "partitions": ["pickup_month"],
            "dataframe": benchmark_df,
            "repartition_count": None,
            "repartition_columns": None,
        },

        "pickup_date": {
            "table":
                benchmark_config["date_partitioned_table"],
            "partitions": ["pickup_date"],
            "dataframe": benchmark_df,
            "repartition_count": None,
            "repartition_columns": None,
        },

        # Random controls: same target Parquet file counts as the three
        # semantic partitioned strategies, but rows are assigned by a
        # deterministic pseudo-random hash rather than a meaningful field.
        "random_128_files": {
            "table":
                benchmark_config["random_borough_table"],
            "partitions": [random_borough_column],
            "dataframe": random_borough_df,
            "repartition_count": random_borough_file_count,
            "repartition_columns": [random_borough_column],
        },

        "random_20_files": {
            "table":
                benchmark_config["random_month_table"],
            "partitions": [random_month_column],
            "dataframe": random_month_df,
            "repartition_count": random_month_file_count,
            "repartition_columns": [random_month_column],
        },

        "random_608_files": {
            "table":
                benchmark_config["random_date_table"],
            "partitions": [random_date_column],
            "dataframe": random_date_df,
            "repartition_count": random_date_file_count,
            "repartition_columns": [random_date_column],
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
            strategy["dataframe"],
            output_path,
            strategy["partitions"],
            runs=write_runs,
            repartition_count=strategy["repartition_count"],
            repartition_columns=strategy["repartition_columns"],
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