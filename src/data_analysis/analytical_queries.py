

from __future__ import annotations

from pyspark.sql import SparkSession

from src.common import read_delta, table_path

import csv
import os
import time
import statistics



from src.common import read_delta, table_path

INTEGRATED_VIEW = "integrated_taxi_trips"


ANALYTICAL_QUERIES = {
    # Q1. Monthly taxi demand for each taxi zone
    "monthly_zone_demand": f"""
        SELECT
            pickup_year,
            pickup_month,
            pickup_location_id,
            pickup_zone,
            pickup_borough,
            COUNT(*) AS trip_count
        FROM {INTEGRATED_VIEW}
        WHERE pickup_zone IS NOT NULL
        GROUP BY
            pickup_year,
            pickup_month,
            pickup_location_id,
            pickup_zone,
            pickup_borough
        ORDER BY
            pickup_year,
            pickup_month,
            trip_count DESC
    """,

    # Q2. Average trip distance under different weather conditions
    "avg_distance_by_weather": f"""
        SELECT
            weather_condition_code,
            COUNT(*) AS trip_count,
            ROUND(AVG(trip_distance), 3) AS avg_trip_distance
        FROM {INTEGRATED_VIEW}
        WHERE weather_available = TRUE
          AND weather_condition_code IS NOT NULL
          AND trip_distance IS NOT NULL
        GROUP BY weather_condition_code
        ORDER BY weather_condition_code
    """,

    # Q3. Relationship between air quality and taxi demand
    "air_quality_demand_relationship": f"""
        WITH hourly_demand AS (
            SELECT
                pickup_hour,
                MAX(pm25_avg_ug_m3) AS pm25_avg_ug_m3,
                COUNT(*) AS trip_count
            FROM {INTEGRATED_VIEW}
            WHERE air_quality_available = TRUE
              AND pm25_avg_ug_m3 IS NOT NULL
            GROUP BY pickup_hour
        )

        SELECT
            COUNT(*) AS hours_analyzed,
            ROUND(AVG(pm25_avg_ug_m3), 3) AS avg_pm25_ug_m3,
            ROUND(AVG(trip_count), 3) AS avg_hourly_trip_count,
            ROUND(
                CORR(pm25_avg_ug_m3, trip_count),
                4
            ) AS pm25_demand_correlation
        FROM hourly_demand
    """,

    # Q4. Taxi zones with the largest variation in demand
    #     under different weather conditions
    "zone_weather_variation": f"""
        WITH weather_hours AS (
            SELECT
                weather_condition_code,
                COUNT(DISTINCT pickup_hour) AS condition_hours
            FROM {INTEGRATED_VIEW}
            WHERE weather_available = TRUE
              AND weather_condition_code IS NOT NULL
            GROUP BY weather_condition_code
        ),

        zone_weather_demand AS (
            SELECT
                pickup_location_id,
                pickup_zone,
                pickup_borough,
                weather_condition_code,
                COUNT(*) AS trip_count
            FROM {INTEGRATED_VIEW}
            WHERE pickup_zone IS NOT NULL
              AND weather_available = TRUE
              AND weather_condition_code IS NOT NULL
            GROUP BY
                pickup_location_id,
                pickup_zone,
                pickup_borough,
                weather_condition_code
        ),

        normalized_demand AS (
            SELECT
                z.pickup_location_id,
                z.pickup_zone,
                z.pickup_borough,
                z.weather_condition_code,
                z.trip_count / w.condition_hours
                    AS avg_hourly_demand
            FROM zone_weather_demand z
            JOIN weather_hours w
              ON z.weather_condition_code =
                 w.weather_condition_code
        )

        SELECT
            pickup_location_id,
            pickup_zone,
            pickup_borough,

            ROUND(
                MAX(avg_hourly_demand)
                - MIN(avg_hourly_demand),
                3
            ) AS demand_variation,

            ROUND(
                STDDEV_POP(avg_hourly_demand),
                3
            ) AS demand_stddev

        FROM normalized_demand

        GROUP BY
            pickup_location_id,
            pickup_zone,
            pickup_borough

        ORDER BY demand_variation DESC
    """,

    # Q5. Peak travel hours for each day of the week
    "peak_hour_by_weekday": f"""
        WITH weekday_hour_demand AS (
            SELECT
                dayofweek(pickup_date) AS weekday_number,
                date_format(pickup_date, 'EEEE') AS weekday,
                hour(pickup_timestamp) AS hour_of_day,
                COUNT(*) AS trip_count
            FROM {INTEGRATED_VIEW}
            GROUP BY
                dayofweek(pickup_date),
                date_format(pickup_date, 'EEEE'),
                hour(pickup_timestamp)
        ),

        ranked AS (
            SELECT
                weekday_number,
                weekday,
                hour_of_day,
                trip_count,

                DENSE_RANK() OVER (
                    PARTITION BY weekday_number
                    ORDER BY trip_count DESC
                ) AS demand_rank

            FROM weekday_hour_demand
        )

        SELECT
            weekday,
            hour_of_day AS peak_hour,
            trip_count
        FROM ranked
        WHERE demand_rank = 1
        ORDER BY weekday_number
    """,

    # Q6. Monthly trends in taxi demand
    "monthly_demand_trend": f"""
        WITH monthly_demand AS (
            SELECT
                pickup_year,
                pickup_month,
                COUNT(*) AS trip_count
            FROM {INTEGRATED_VIEW}
            GROUP BY
                pickup_year,
                pickup_month
        ),

        monthly_change AS (
            SELECT
                pickup_year,
                pickup_month,
                trip_count,

                LAG(trip_count) OVER (
                    ORDER BY
                        pickup_year,
                        pickup_month
                ) AS previous_month_trip_count

            FROM monthly_demand
        )

        SELECT
            pickup_year,
            pickup_month,
            trip_count,

            trip_count - previous_month_trip_count
                AS month_over_month_change,

            CASE
                WHEN previous_month_trip_count IS NULL
                  OR previous_month_trip_count = 0
                THEN NULL

                ELSE ROUND(
                    100.0
                    * (trip_count - previous_month_trip_count)
                    / previous_month_trip_count,
                    2
                )
            END AS month_over_month_change_pct

        FROM monthly_change

        ORDER BY
            pickup_year,
            pickup_month
    """
}

def run_analytical_queries(
    spark: SparkSession,
    config: dict,
) -> None:

    integrated_path = config[
        "data_integration"
    ][
        "integrated_taxi_trips_table"
    ]

    integrated_df = read_delta(
        spark,
        table_path(
            config,
            integrated_path,
        ),
    )

    integrated_df.createOrReplaceTempView(
        INTEGRATED_VIEW
    )

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

    timing_results = []

    for query_name, query in ANALYTICAL_QUERIES.items():

        print(
            f"\n=== {query_name} ==="
        )

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