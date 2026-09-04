from pyspark.sql import SparkSession


def build_integrated_tables(spark: SparkSession, config: dict) -> None:
    """Join taxi trips with weather, air quality, and taxi zones."""
    pass
