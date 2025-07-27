from source.utils.spark_utils import create_session
import source.utils.common_utils as cu


def clean_sf_fire_service_calls(file_name: str):
    """Create a spark session"""
    spark_session = create_session('clean-sf-fire-service-calls')

    file_path = cu.get_file_path(file_name)

    """Read a file"""
    sf_fire_service_df = spark_session.read \
        .format("csv") \
        .option("header", True) \
        .option("inferSchema", False) \
        .load(file_path)

    print(sf_fire_service_df.count())

    spark_session.stop()


if __name__ == "__main__":
    file = 'Fire_Service_Calls_20250727.csv'
    clean_sf_fire_service_calls(file)

    print('Completed Well!')
