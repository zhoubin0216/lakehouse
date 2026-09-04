NUMBER_OF_TRIPS_PER_BOROUGH = """
SELECT z.borough, COUNT(*) AS trip_count
FROM taxi_trips t
JOIN dim_taxi_zone z
  ON t.pickup_location_id = z.location_id
GROUP BY z.borough
"""

AVERAGE_TRIP_DURATION_PER_DAY = """
SELECT pickup_date, AVG(trip_duration_minutes) AS avg_trip_duration_minutes
FROM taxi_trips
GROUP BY pickup_date
"""

AVERAGE_FARE_PER_BOROUGH = """
SELECT z.borough, AVG(t.fare_amount) AS avg_fare
FROM taxi_trips t
JOIN dim_taxi_zone z
  ON t.pickup_location_id = z.location_id
GROUP BY z.borough
"""
