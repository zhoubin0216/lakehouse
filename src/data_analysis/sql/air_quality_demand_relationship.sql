WITH demand AS (
    SELECT pickup_hour, COUNT(*) AS trip_count
    FROM integrated_taxi_trips GROUP BY pickup_hour
), hourly_demand AS (
    SELECT a.pm25_avg_ug_m3, COALESCE(d.trip_count, 0) AS trip_count
    FROM analysis_air_quality a
    LEFT JOIN demand d ON a.event_hour = d.pickup_hour
    WHERE a.pm25_avg_ug_m3 IS NOT NULL
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
