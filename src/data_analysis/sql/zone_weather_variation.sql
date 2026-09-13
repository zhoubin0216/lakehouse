WITH weather_hours AS (
    SELECT weather_condition_code, COUNT(*) AS condition_hours
    FROM analysis_weather
    WHERE weather_condition_code IS NOT NULL
    GROUP BY weather_condition_code
), zones AS (
    SELECT DISTINCT pickup_location_id, pickup_zone, pickup_borough
    FROM integrated_taxi_trips WHERE pickup_zone IS NOT NULL
), zone_weather_demand AS (
    SELECT t.pickup_location_id, w.weather_condition_code, COUNT(*) AS trip_count
    FROM integrated_taxi_trips t
    JOIN analysis_weather w ON t.pickup_hour = w.event_hour
    WHERE w.weather_condition_code IS NOT NULL
    GROUP BY t.pickup_location_id, w.weather_condition_code
), normalized_demand AS (
    SELECT z.pickup_location_id, z.pickup_zone, z.pickup_borough,
           w.weather_condition_code,
           COALESCE(d.trip_count, 0) * 1.0 / w.condition_hours AS avg_hourly_demand
    FROM zones z CROSS JOIN weather_hours w
    LEFT JOIN zone_weather_demand d
      ON z.pickup_location_id = d.pickup_location_id
     AND w.weather_condition_code = d.weather_condition_code
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
