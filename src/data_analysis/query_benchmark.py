from __future__ import annotations

import argparse
import csv
import os
import time
import statistics
from pathlib import Path
from pyspark.sql import SparkSession
from src.common import create_spark, load_config, table_path
from src.data_analysis.analytical_queries import register_analysis_views
from src.data_analysis.query_library import ANALYTICAL_QUERIES

def run_query_benchmark(
    spark: SparkSession,
    config: dict,
) -> None:

    register_analysis_views(spark, config)

    baseline_root = config[
        "data_analysis"
    ][
        "baseline_results_root"
    ]

    timing_file = table_path(
        config,
        config[
            "data_analysis"
        ][
            "baseline_timing_file"
        ],
    )

    warmup_runs = config[
        "benchmark"
    ][
        "warmup_runs"
    ]

    measured_runs = config[
        "benchmark"
    ][
        "measured_runs"
    ]

    if warmup_runs < 0 or measured_runs < 1:
        raise ValueError("warmup_runs must be non-negative and measured_runs must be positive")
    timing_results = []

    for query_name, query in ANALYTICAL_QUERIES.items():

        print(
            f"\n=== {query_name} ==="
        )

        plan_path = Path(table_path(config, f"{baseline_root}/{query_name}_plan.txt"))
        plan_path.parent.mkdir(parents=True, exist_ok=True)
        plan = spark.sql("EXPLAIN FORMATTED " + query).collect()
        plan_path.write_text("\n".join(row[0] for row in plan), encoding="utf-8")

        # =================================================
        # Warm-up runs
        # =================================================
        print(
            f"Warm-up runs: {warmup_runs}"
        )

        for i in range(warmup_runs):

            result = spark.sql(query)

            start_time = time.perf_counter()

            # Spark action: actually executes the SQL
            result.collect()

            elapsed = (
                time.perf_counter()
                - start_time
            )

            print(
                f"Warm-up {i + 1}: "
                f"{elapsed:.3f} seconds"
            )

        # =================================================
        # Measured runs
        # =================================================
        print(
            f"Measured runs: {measured_runs}"
        )

        run_times = []

        for i in range(measured_runs):

            result = spark.sql(query)

            start_time = time.perf_counter()

            # Force SQL execution
            result.collect()

            elapsed = (
                time.perf_counter()
                - start_time
            )

            run_times.append(
                elapsed
            )

            print(
                f"Run {i + 1}: "
                f"{elapsed:.3f} seconds"
            )

        # =================================================
        # Timing statistics
        # =================================================
        mean_time = statistics.mean(
            run_times
        )

        median_time = statistics.median(
            run_times
        )

        min_time = min(
            run_times
        )

        max_time = max(
            run_times
        )

        print(
            f"Mean execution time: "
            f"{mean_time:.3f} seconds"
        )

        print(
            f"Median execution time: "
            f"{median_time:.3f} seconds"
        )

        print(
            f"Min execution time: "
            f"{min_time:.3f} seconds"
        )

        print(
            f"Max execution time: "
            f"{max_time:.3f} seconds"
        )

        # =================================================
        # Store timing information
        # =================================================
        timing_record = {
            "query_name": query_name,
            "warmup_runs": warmup_runs,
            "measured_runs": measured_runs,
        }

        for i, run_time in enumerate(
            run_times,
            start=1,
        ):
            timing_record[
                f"run_{i}_seconds"
            ] = round(
                run_time,
                3,
            )

        timing_record.update({
            "mean_seconds": round(
                mean_time,
                3,
            ),
            "median_seconds": round(
                median_time,
                3,
            ),
            "min_seconds": round(
                min_time,
                3,
            ),
            "max_seconds": round(
                max_time,
                3,
            ),
        })

        timing_results.append(
            timing_record
        )

        # =================================================
        # Generate result for display and storage
        #
        # This execution is NOT included in benchmark time.
        # Keep the result inside Spark instead of collecting
        # rows to Python and recreating a DataFrame.
        # =================================================
        output_df = spark.sql(query).cache()

        # Materialize the cache once.
        output_df.count()

        # Display result.
        output_df.show(
            50,
            truncate=False,
        )

        # =================================================
        # Save baseline result as Delta
        # =================================================
        output_path = table_path(
            config,
            f"{baseline_root}/{query_name}",
        )

        output_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(output_path)

        # Release cached result before the next query.
        output_df.unpersist()

        print(
            f"Baseline result saved to: "
            f"{output_path}"
        )

    # =====================================================
    # Save timing results to CSV
    # =====================================================
    timing_directory = os.path.dirname(
        timing_file
    )

    os.makedirs(
        timing_directory,
        exist_ok=True,
    )

    fieldnames = [
        "query_name",
        "warmup_runs",
        "measured_runs",
    ]

    fieldnames.extend(
        [
            f"run_{i}_seconds"
            for i in range(
                1,
                measured_runs + 1,
            )
        ]
    )

    fieldnames.extend([
        "mean_seconds",
        "median_seconds",
        "min_seconds",
        "max_seconds",
    ])

    with open(
        timing_file,
        "w",
        newline="",
        encoding="utf-8",
    ) as csv_file:

        writer = csv.DictWriter(
            csv_file,
            fieldnames=fieldnames,
        )

        writer.writeheader()

        writer.writerows(
            timing_results
        )

    print(
        f"\nBaseline query timings saved to: "
        f"{timing_file}"
    )

def main():
    parser = argparse.ArgumentParser(description="Benchmark the analytical query library")
    parser.add_argument("--config", type=Path, default=Path("configs/config.yaml"))
    args = parser.parse_args()
    config = load_config(args.config)
    spark = create_spark()
    try:
        run_query_benchmark(spark, config)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
