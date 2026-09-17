"""Meteostat weather-condition semantics shared by products and reports."""
from __future__ import annotations

from itertools import chain

from pyspark.sql import Column
from pyspark.sql import functions as F


# Meteostat ``coco`` values: https://dev.meteostat.net/formats.html
METEOSTAT_WEATHER_CONDITIONS = {
    1: "Clear",
    2: "Fair",
    3: "Cloudy",
    4: "Overcast",
    5: "Fog",
    6: "Freezing Fog",
    7: "Light Rain",
    8: "Rain",
    9: "Heavy Rain",
    10: "Freezing Rain",
    11: "Heavy Freezing Rain",
    12: "Sleet",
    13: "Heavy Sleet",
    14: "Light Snowfall",
    15: "Snowfall",
    16: "Heavy Snowfall",
    17: "Rain Shower",
    18: "Heavy Rain Shower",
    19: "Sleet Shower",
    20: "Heavy Sleet Shower",
    21: "Snow Shower",
    22: "Heavy Snow Shower",
    23: "Lightning",
    24: "Hail",
    25: "Thunderstorm",
    26: "Heavy Thunderstorm",
    27: "Storm",
}


def weather_condition_name(code: int | None) -> str:
    """Return a stable human-readable label for one Meteostat condition code."""
    if code is None:
        return "Unknown"
    return METEOSTAT_WEATHER_CONDITIONS.get(code, f"Unknown ({code})")


def weather_condition_name_column(column: str = "weather_condition_code") -> Column:
    """Return a Spark expression mapping a Meteostat code to its label."""
    entries = chain.from_iterable(
        (F.lit(code), F.lit(label))
        for code, label in METEOSTAT_WEATHER_CONDITIONS.items()
    )
    mapping = F.create_map(*entries)
    code = F.col(column).cast("int")
    return F.coalesce(
        F.element_at(mapping, code),
        F.concat(F.lit("Unknown ("), code.cast("string"), F.lit(")")),
    )
