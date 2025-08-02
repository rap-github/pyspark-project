from pyspark.sql import DataFrame
import pyspark.sql.functions as sf


def transform_date(df: DataFrame, col_names: list, date_format: str = 'MM/dd/yyyy') -> DataFrame:
    """Transforms col to specified date format"""
    for col_name in col_names:
        df = df.withColumn(col_name, sf.to_date(sf.col(col_name), date_format))

    return df


def transform_datetime(df: DataFrame, col_names: list, datetime_format: str = 'MM/dd/yyyy') -> DataFrame:
    for col_name in col_names:
        df = df.withColumn(col_name, sf.to_timestamp(sf.col(col_name), datetime_format))

    return df

