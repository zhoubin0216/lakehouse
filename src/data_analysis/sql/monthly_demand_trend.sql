WITH monthly_demand AS (
    SELECT
        pickup_year,
        pickup_month,
        COUNT(*) AS trip_count
    FROM integrated_taxi_trips
    GROUP BY
        pickup_year,
        pickup_month
),

monthly_change AS (
    SELECT m.pickup_year, m.pickup_month, m.trip_count,
           p.trip_count AS previous_month_trip_count
    FROM monthly_demand m
    LEFT JOIN monthly_demand p
      ON make_date(p.pickup_year, p.pickup_month, 1)
         = add_months(make_date(m.pickup_year, m.pickup_month, 1), -1)
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
