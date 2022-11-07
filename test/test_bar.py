from pyspark.sql import SparkSession

def test_bar(spark: SparkSession):
    spark.sql("create database foo")
    (
        spark
        .range(50)
        .toDF("foo")
        .selectExpr("foo", "foo % 4 as bar")
        .write
        .saveAsTable("foo.my_table")
    )

    file_path, *_ = (row["file"] for row in spark.table("foo.my_table").selectExpr("input_file_name() as file").collect())

    assert "test_bar" in file_path
