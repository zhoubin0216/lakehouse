import pytest
from pyspark.sql import SparkSession

from src.common import create_spark


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = create_spark()
    yield session
    session.stop()