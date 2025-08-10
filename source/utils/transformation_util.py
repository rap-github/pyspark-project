from pyspark.sql import DataFrame
import pyspark.sql.functions as sf


def transform_date(df: DataFrame, col_names: list, date_format: str = 'MM/dd/yyyy') -> DataFrame:
    """Transforms col to specified date format"""
    for col_name in col_names:
        df = df.withColumn(col_name, sf.to_date(sf.col(col_name), date_format))

    return df


def transform_datetime(df: DataFrame,
                       col_names: list,
                       datetime_format: str = 'MM/dd/yyyy') -> DataFrame:
    """Transforms col to specified datetime format"""
    for col_name in col_names:
        df = df.withColumn(col_name, sf.to_timestamp(sf.col(col_name), datetime_format))

    return df


def change_case(df: DataFrame,
                col_names: list,
                case_format: str) -> DataFrame:
    """Transforms col to specified case"""
    for col_name in col_names:
        if case_format == 'U':
            df = df.withColumn(col_name, sf.upper(sf.col(col_name)))
        elif case_format == 'L':
            df = df.withColumn(col_name, sf.lower(sf.col(col_name)))
        elif case_format == 'I':
            df = df.withColumn(col_name, sf.initcap(sf.col(col_name)))

    return df

