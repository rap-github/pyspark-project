from pyspark.sql import SparkSession
from source.utils.schema_util import *


def set_spark_session_config(spark_session: SparkSession):
    spark_session.conf.set("spark.sql.debug.maxToStringFields", 1000)


def get_schema(name: str) -> str:
    return sf_fire_service_calls
