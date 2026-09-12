from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


SCALAR_SCHEMA_VERSION_COLUMNS = {
    "taxi_schema_version": "taxi_schema_versions",
    "pickup_zone_schema_version": "pickup_zone_schema_versions",
    "dropoff_zone_schema_version": "dropoff_zone_schema_versions",
    "weather_schema_version": "weather_schema_versions",
}
ARRAY_SCHEMA_VERSION_COLUMNS = ["air_quality_schema_versions"]


def schema_version_aggregations(df: DataFrame) -> list[Column]:
    """Build aggregations that summarize all source versions in a DataFrame."""
    required_columns = [
        *SCALAR_SCHEMA_VERSION_COLUMNS,
        *ARRAY_SCHEMA_VERSION_COLUMNS,
    ]
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Input is missing schema-version columns: {missing_columns}")

    aggregations = [
        F.sort_array(F.collect_set(source)).alias(target)
        for source, target in SCALAR_SCHEMA_VERSION_COLUMNS.items()
    ]
    empty_versions = F.array().cast("array<int>")
    aggregations.extend(
        F.sort_array(
            F.array_distinct(
                F.flatten(
                    F.collect_list(
                        F.coalesce(F.col(column), empty_versions)
                    )
                )
            )
        ).alias(column)
        for column in ARRAY_SCHEMA_VERSION_COLUMNS
    )
    return aggregations


def collect_schema_version_snapshot(df: DataFrame) -> dict[str, list[int]]:
    """Collect one compact source-version snapshot for reporting."""
    row = df.agg(*schema_version_aggregations(df)).first()
    return row.asDict() if row is not None else {}
