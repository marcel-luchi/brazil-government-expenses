from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.spark_utils import SparkUtils
import os
import helpers.constants as constants


class DataQualityOperator(BaseOperator):
    """Airflow operator for data quality validations

    Args:
        table (str): table in which data will be validated.
        bucket (str): bucket name containing the parquet files
        validation_type (str): fact_keys: Validate fact partition for having no more rows having null keys
                               than the threshold (in percent).
                               fact_rows: Validate fact partition for having number of rows greater than the
                               minimum threshold.
                               dimension_duplicate_keys: Validates dimension tables for having no duplicate rows
                               based on a given key.
                               dimension_rows: Validate dimension table for having number of rows greater
                               than the minimum threshold.
        validation_threshold (int): minimum number of records for a table to be considered valid
        """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 table,
                 bucket,
                 validation_type,
                 validation_threshold=0,
                 keys_list=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.bucket = bucket
        self.validation_type = validation_type
        self.validation_threshold = validation_threshold
        self.keys_list = keys_list

    def __validate_fact_null_keys(self, df, year, month):
        """Validate fact table for having rows containing null keys.
         Table is validated if percentage of rows containing null keys is less than the threshold."""
        df = df.where(f"year == {year} and month == {month}")
        total_records = df.count()
        if total_records == 0:
            raise ValueError(f"No records in table {self.table}.")

        i = 0
        where = ''
        for item in self.keys_list:
            if i == 0:
                where = where + f'{item} is null'
            else:
                where = where + f" or {item} is null"
            i += 1

        if i == 0:
            raise KeyError("Please inform table keys to be validated.")

        invalid_records = float(df.where(where).count()) * 100

        if invalid_records / total_records > self.validation_threshold:
            raise KeyError(f"Number of null keys in table {self.table} higher than \
            threshold {self.validation_threshold}.")
        self.log.info(f"Table: {self.table} passed key error validation")
        self.log.info(f'Null keys percentage: {invalid_records / total_records}')

    def __validate_rows_num(self, df):
        """Validate if table has more rows than the threshold."""
        if df.count() < self.validation_threshold:
            raise ValueError(f"Table: {self.table} has not reached minimum records to be validated.")

    def __validate_duplicate_keys(self, df):
        """Validate dimension tables based on a given key for having duplicate rows."""
        self.log.info(f'Table Keys: {self.keys_list}')
        if df.groupBy(self.keys_list).count().where("count > 1").count() > 0:
            raise KeyError (f"Duplicate keys found in table {self.table}")
        self.log.info(f"Table: {self.table} validated for duplicate keys.")

    def execute(self, context):
        """Method called by Airflow Task."""
        year = '{execution_date.year}'.format(**context)
        month = '{execution_date.month}'.format(**context)

        spark = SparkUtils.create_spark_session()
        params = {'fact_keys': {'table_path': constants.FACT_DIR,
                                'method': self.__validate_fact_null_keys},
                  'fact_rows': {'table_path': constants.FACT_DIR,
                                'method': self.__validate_rows_num},
                  'dimension_rows': {'table_path': constants.DIMENSION_DIR,
                                     'method': self.__validate_rows_num},
                  'dimension_duplicate_keys': {'table_path': constants.DIMENSION_DIR,
                                               'method': self.__validate_duplicate_keys}
                  }

        method = params.get(self.validation_type).get('method')
        table_path = params.get(self.validation_type).get('table_path')
        path = os.path.join(self.bucket,
                            table_path,
                            self.table)
        df = spark.read.parquet(path)
        self.log.info("Beggining data validation:")
        self.log.info(f"Validation type: {self.validation_type}")
        self.log.info(f"Table: {self.table}")
        if method == self.__validate_fact_null_keys :
            method(df, year, month)
        else:
            method(df)
