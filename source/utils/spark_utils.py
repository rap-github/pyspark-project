from pyspark.sql import SparkSession


def create_session(app_name: str) -> SparkSession:
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

    return spark
