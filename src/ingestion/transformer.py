from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def standardize_columns(df: DataFrame, column_mapping: dict[str, str] | None) -> DataFrame:
    if not column_mapping:
        return df

    result = df
    for source, target in column_mapping.items():
        if source in result.columns and source != target:
            result = result.withColumnRenamed(source, target)
    return result


def add_time_partitions(df: DataFrame, timestamp_col: str, prefix: str) -> DataFrame:
    return (
        df.withColumn(f"{prefix}_year", F.year(F.col(timestamp_col)))
        .withColumn(f"{prefix}_month", F.month(F.col(timestamp_col)))
        .withColumn(f"{prefix}_date", F.to_date(F.col(timestamp_col)))
        .withColumn(f"{prefix}_hour", F.date_trunc("hour", F.col(timestamp_col)))
    )
