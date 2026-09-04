from pyspark.sql import DataFrame


def write_delta(df: DataFrame, path: str, mode: str = "overwrite", partition_columns: list[str] | None = None) -> None:
    writer = df.write.format("delta").mode(mode)
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    writer.save(path)
