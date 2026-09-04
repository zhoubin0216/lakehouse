from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def count_missing_primary_keys(df: DataFrame, primary_key: list[str] | None) -> int:
    if not primary_key:
        return 0
    condition = None
    for column in primary_key:
        column_condition = F.col(column).isNull()
        condition = column_condition if condition is None else condition | column_condition
    return df.filter(condition).count()
