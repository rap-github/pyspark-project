from source.utils.spark_util import create_session
import source.utils.common_util as cmu
import source.utils.transformation_util as tru
import source.utils.config_util as cou

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType
import pyspark.sql.functions as sf

def clean_sf_fire_service_calls(file_name: str):
    """Create a spark session"""
    spark_session = create_session('clean-sf-fire-service-calls')
    cou.set_spark_session_config(spark_session)

    file_path = cmu.get_file_path(file_name)

    """Define the schema"""
    schema = StructType([StructField('CallNumber', IntegerType(), True),
                         StructField('UnitId', StringType(), True),
                         StructField('IncidentNumber', IntegerType(), True),
                         StructField('CallType', StringType(), True),
                         StructField('CallDate', StringType(), True),
                         StructField('WatchDate', StringType(), True),
                         StructField('ReceivedDtTm', StringType(), True),
                         StructField('EntryDtTm', StringType(), True),
                         StructField('DispatchDtTm', StringType(), True),
                         StructField('ResponseDtTm', StringType(), True),
                         StructField('OnSceneDtTm', StringType(), True),
                         StructField('TransportDtTm', StringType(), True),
                         StructField('HospitalDtTm', StringType(), True),
                         StructField('CallFinalDisposition', StringType(), True),
                         StructField('AvailableDtTm', StringType(), True),
                         StructField('Address', StringType(), True),
                         StructField('City', StringType(), True),
                         StructField('ZipCode', IntegerType(), True),
                         StructField('Battalion', StringType(), True),
                         StructField('StationArea', StringType(), True),
                         StructField('Box', IntegerType(), True),
                         StructField('OriginalPriority', IntegerType(), True),
                         StructField('Priority', IntegerType(), True),
                         StructField('FinalPriority', IntegerType(), True),
                         StructField('ALSUnit', StringType(), True),
                         StructField('CallTypeGroup', StringType(), True),
                         StructField('NumberOfAlarms', IntegerType(), True),
                         StructField('UnitType', StringType(), True),
                         StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                         StructField('FirePreventionDistrict', IntegerType(), True),
                         StructField('SupervisorDistrict', IntegerType(), True),
                         StructField('NeighborhoodsAnalysisBoundaries', StringType(), True),
                         StructField('RowId', StringType(), True),
                         StructField('CaseLocation', StringType(), True),
                         StructField('DataAsOf', StringType(), True),
                         StructField('DataLoadedAt', StringType(), True),
                         StructField('Analysis Neighborhoods', IntegerType(), True)])

    """Read a file"""
    sf_fire_service_df = spark_session.read \
        .format("csv") \
        .option("delimiter", ",") \
        .option("header", True) \
        .option("inferSchema", False) \
        .schema(schema) \
        .load(file_path)

    print(sf_fire_service_df.count())
    sf_fire_service_df.printSchema()
    sf_fire_service_df.show(10)

    """Apply date transformation"""
    date_fields = ['CallDate', 'WatchDate']
    sf_fire_service_df = tru.transform_date(sf_fire_service_df, date_fields, 'MM/dd/yyyy')

    """Apply datetime transformation"""
    datetime_fields = ['ReceivedDtTm', 'EntryDtTm', 'DispatchDtTm', 'ResponseDtTm',
                       'OnSceneDtTm', 'TransportDtTm', 'HospitalDtTm',
                       'AvailableDtTm', 'DataLoadedAt']

    sf_fire_service_df = tru.transform_datetime(sf_fire_service_df, datetime_fields, 'MM/dd/yyyy HH:mm:SS a')

    sf_fire_service_df.printSchema()
    sf_fire_service_df.show()

    spark_session.stop()


if __name__ == "__main__":
    file = 'Fire_Service_Calls_20250727.csv'
    clean_sf_fire_service_calls(file)

    print('Completed Well!')
