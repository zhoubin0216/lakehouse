from pyspark.sql import SparkSession


def build_raw_tables(spark: SparkSession, config: dict) -> None:
    """Load source CSV/Parquet files and write raw Delta tables."""
    pass
