from pyspark.sql import SparkSession
import pytest

@pytest.fixture(scope="module")
def warehouse_dir(request: pytest.FixtureRequest, tmpdir_factory: pytest.TempdirFactory):
    tmpdir = tmpdir_factory.mktemp(request.module.__name__)
    yield tmpdir
    tmpdir

@pytest.fixture(scope="module")
def spark(warehouse_dir):
    _spark = (
        SparkSession
        .builder
        .master("local[1]")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.executor.memory", "512m")
        .getOrCreate()
    )

    yield _spark

    _spark.stop()
