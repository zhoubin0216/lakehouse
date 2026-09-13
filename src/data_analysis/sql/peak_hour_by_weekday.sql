WITH weekday_hour_demand AS (
    SELECT
        dayofweek(pickup_date) AS weekday_number,
        date_format(pickup_date, 'EEEE') AS weekday,
        hour(pickup_timestamp) AS hour_of_day,
        COUNT(*) AS trip_count
    FROM integrated_taxi_trips
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
