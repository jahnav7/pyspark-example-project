"""
etl_job_faulty.py
~~~~~~~~~~~~~~~~~

This Python module contains an ETL job with intentional faults for
demonstration purposes.
"""

from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, lit
from pyspark.sql import DataFrame  # Unused import

from dependencies.spark import start_spark


def main():
    """Main ETL script definition.

    :return: None
    """
    # Start Spark application and get Spark session, logger, and config
    spark, log, config = start_spark(
        app_name='my_faulty_etl_job', files=['configs/etl_config.json'])

    # Log the start of the job
    log.info('ETL job is starting')  # Use of inconsistent logging levels

    # Execute ETL pipeline
    data = extract_data(spark)
    data_transformed = transform_data(data, config.get('steps_per_floor', 10))  # Default may not match requirements
    load_data(data_transformed)

    # Terminate Spark application
    log.info('ETL job finished successfully')
    spark.stop()  # Missing error handling for spark.stop()
    return None


def extract_data(spark):
    """Load data from an unsupported file format (faulty).

    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    df = (
        spark
        .read
        .json('tests/test_data/employees.json')  # Assuming JSON format but code expects Parquet
    )

    return df


def transform_data(df, steps_per_floor_):
    """Transform dataset with unnecessary computations.

    :param df: Input DataFrame.
    :param steps_per_floor_: Steps per floor parameter.
    :return: Transformed DataFrame.
    """
    df_transformed = (
        df
        .select(
            col('id'),
            concat_ws(
                ' ',
                col('first_name'),
                col('second_name')).alias('name'),
               (col('floor') * lit(steps_per_floor_) + lit(0)).alias('steps_to_desk')  # Redundant addition of 0
        )
    )

    return df_transformed


def load_data(df):
    """Load data with hard-coded paths and inefficient operations.

    :param df: DataFrame to save.
    :return: None
    """
    # Inefficient save operation: Repartitioning unnecessarily
    (df
     .repartition(10)
     .write
     .csv('hardcoded_path/loaded_data', mode='overwrite', header=True))  # Hardcoded path
    return None


def create_test_data(spark):
    """Create faulty test data.

    :return: None
    """
    # Create example data with potential schema issues
    local_records = [
        Row(id=1, first_name='Dan', floor=1),  # Missing 'second_name'
        Row(id=2, first_name=None, second_name='Sommerville', floor=1),  # Missing first_name
        Row(id=3, first_name='Alex', second_name='Ioannides', floor='2'),  # 'floor' as string instead of integer
    ]

    df = spark.createDataFrame(local_records)

    # Save data
    (df
     .coalesce(1)
     .write
     .parquet('tests/test_data/employees', mode='overwrite'))

    return None


# Entry point for the PySpark ETL application
if __name__ == '__main__':
    main()
