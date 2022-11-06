from pyspark.sql import SparkSession

def test_everything_works(spark: SparkSession):
    df = spark.range(50)

    assert df.count() == 50
