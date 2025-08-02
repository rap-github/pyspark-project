from pyspark.sql import SparkSession


def set_spark_session_config(spark_session: SparkSession):
    spark_session.conf.set("spark.sql.debug.maxToStringFields", 1000)
