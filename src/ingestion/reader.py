from pyspark.sql import DataFrame, SparkSession


def read_dataset(spark: SparkSession, dataset_config: dict) -> DataFrame:
    fmt = dataset_config["format"].lower()
    path = dataset_config["source_path"]

    if fmt == "csv":
        return spark.read.option("header", True).option("inferSchema", True).csv(path)
    if fmt == "parquet":
        return spark.read.parquet(path)

    raise ValueError(f"Unsupported dataset format: {fmt}")
