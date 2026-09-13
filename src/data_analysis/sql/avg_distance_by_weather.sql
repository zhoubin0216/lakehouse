SELECT
    weather_condition_code,
    COUNT(*) AS trip_count,
    ROUND(AVG(trip_distance), 3) AS avg_trip_distance
FROM integrated_taxi_trips
WHERE weather_condition_code IS NOT NULL
  AND trip_distance IS NOT NULL
GROUP BY weather_condition_code
ORDER BY weather_condition_code
