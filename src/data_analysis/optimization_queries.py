"""Equivalent experimental SQL; original Task 1/2 files stay unchanged."""
from datetime import date

from src.data_analysis.query_library import ANALYTICAL_QUERIES


def replace_once(sql, old, new):
    if sql.count(old) != 1:
        raise ValueError("Original SQL contract changed; review optimization before rerunning")
    return sql.replace(old, new, 1)


def broadcast_queries(queries=ANALYTICAL_QUERIES):
    result = dict(queries)
    name = "air_quality_demand_relationship"
    if name in result:
        result[name] = replace_once(result[name], "SELECT a.pm25_avg_ug_m3",
                                    "SELECT /*+ BROADCAST(d) */ a.pm25_avg_ug_m3")
    name = "zone_weather_variation"
    if name in result:
        sql = replace_once(result[name], "SELECT t.pickup_location_id",
                           "SELECT /*+ BROADCAST(w) */ t.pickup_location_id")
        result[name] = replace_once(sql, "SELECT z.pickup_location_id",
                                    "SELECT /*+ BROADCAST(d) */ z.pickup_location_id")
    name = "monthly_demand_trend"
    if name in result:
        result[name] = replace_once(result[name], "SELECT m.pickup_year",
                                    "SELECT /*+ BROADCAST(p) */ m.pickup_year")
    return result


def pruning_queries(year=2024, month=1):
    first = date(year, month, 1)
    next_month = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
    base = ANALYTICAL_QUERIES["monthly_zone_demand"]
    conditions = {
        "monthly_zone_function_filter":
            f"date_format(pickup_timestamp, 'yyyy-MM') = '{first:%Y-%m}'",
        "monthly_zone_range_filter":
            f"pickup_timestamp >= TIMESTAMP_NTZ '{first} 00:00:00' "
            f"AND pickup_timestamp < TIMESTAMP_NTZ '{next_month} 00:00:00'",
    }
    baseline, pruned = {}, {}
    for name, condition in conditions.items():
        baseline[name] = replace_once(base, "WHERE pickup_zone IS NOT NULL",
                                     f"WHERE pickup_zone IS NOT NULL AND ({condition})")
        pruned[name] = replace_once(baseline[name], "WHERE pickup_zone IS NOT NULL",
                                   f"WHERE pickup_year = {year} AND pickup_month = {month} "
                                   "AND pickup_zone IS NOT NULL")
    return baseline, pruned
