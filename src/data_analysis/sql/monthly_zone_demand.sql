SELECT
    pickup_year,
    pickup_month,
    pickup_location_id,
    pickup_zone,
    pickup_borough,
    COUNT(*) AS trip_count
FROM integrated_taxi_trips
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
