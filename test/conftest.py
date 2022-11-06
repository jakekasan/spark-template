from pyspark.sql import SparkSession
import pytest

@pytest.fixture
def spark():
    _spark = (
        SparkSession
        .builder
        .master("local[1]")
        .config("spark.executor.memory", "512m")
        .getOrCreate()
    )

    yield _spark

    _spark.stop()
