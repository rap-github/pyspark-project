import source.utils.spark_util as spu
import source.utils.common_util as cmu
import source.utils.transformation_util as tru
import source.utils.config_util as cou
import source.utils.schema_util as scu

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, BooleanType
from pyspark.sql import DataFrame


def transform_sf_fire_service_calls(raw_df: DataFrame):
    """Apply date transformation"""
    date_fields = ['CallDate', 'WatchDate']
    transformed_df = tru.transform_date(raw_df, date_fields, 'MM/dd/yyyy')

    """Apply datetime transformation"""
    datetime_fields = ['ReceivedDtTm', 'EntryDtTm', 'DispatchDtTm', 'ResponseDtTm',
                       'OnSceneDtTm', 'TransportDtTm', 'HospitalDtTm',
                       'AvailableDtTm', 'DataLoadedAt']

    transformed_df = tru.transform_datetime(transformed_df, datetime_fields, 'MM/dd/yyyy HH:mm:SS a')

    initcase_fields = ['Address', 'UnitType']

    transformed_df = tru.change_case(transformed_df, initcase_fields, 'I')

    return transformed_df


def clean_sf_fire_service_calls(folder_name: str,
                                file_name: str):
    """Create a spark session"""
    spark_session = spu.create_session('clean-sf-fire-service-calls')
    cou.set_spark_session_config(spark_session)

    spark_conf = spark_session.sparkContext.getConf()
    print("Executor Memory: ", spark_conf.get("spark.executor.memory", "Not Set"))
    print("Driver Memory: ", spark_conf.get("spark.driver.memory", "Not Set"))
    print("Executor Cores: ", spark_conf.get("spark.executor.cores", "Not Set"))
    print("Executor Instances: ", spark_conf.get("spark.executor.instances", "Not Set"))

    """Get input/output file path"""
    input_file_path = cmu.get_raw_file_path(folder_name, file_name)
    output_file_path = cmu.get_refined_file_path(folder_name, file_name.split('.')[0])

    """Read a file"""
    sf_fire_service_df = spark_session.read \
        .format("csv") \
        .option("delimiter", ",") \
        .option("header", True) \
        .option("inferSchema", False) \
        .schema(scu.sf_fire_service_calls) \
        .load(input_file_path)

    print(sf_fire_service_df.count())
    #sf_fire_service_df.printSchema()
    #sf_fire_service_df.show(10)

    sf_fire_service_df = transform_sf_fire_service_calls(sf_fire_service_df)

    #sf_fire_service_df.printSchema()
    #sf_fire_service_df.show(10)
    sf_fire_service_df = sf_fire_service_df.repartition(10)
    print(sf_fire_service_df.rdd.getNumPartitions())

    sf_fire_service_df.write \
        .mode('overwrite') \
        .format('parquet') \
        .save(output_file_path)

    user_input = input("Press Enter: ")

    spark_session.stop()

    return sf_fire_service_df


if __name__ == "__main__":
    folder = 'sf_fire_service_calls'
    file = 'Fire_Service_Calls_20250727.csv'
    cleaned_df = clean_sf_fire_service_calls(folder, file)

    print('Completed Well!')

