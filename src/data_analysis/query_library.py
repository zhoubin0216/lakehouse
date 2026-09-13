"""Single registry of the Week 2 query names and their Spark SQL files."""
from pathlib import Path

INTEGRATED_VIEW = "integrated_taxi_trips"
SQL_DIRECTORY = Path(__file__).with_name("sql")

# Order follows Week 2 Task 1. File names and CLI names use the same identifiers.
QUERY_NAMES = (
    "monthly_zone_demand",
    "avg_distance_by_weather",
    "air_quality_demand_relationship",
    "zone_weather_variation",
    "peak_hour_by_weekday",
    "monthly_demand_trend",
)


def load_queries():
    return {name: (SQL_DIRECTORY / f"{name}.sql").read_text(encoding="utf-8")
            for name in QUERY_NAMES}


ANALYTICAL_QUERIES = load_queries()
