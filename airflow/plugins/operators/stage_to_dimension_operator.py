import os
from helpers.schemas import Schemas
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.spark_utils import SparkUtils
from helpers.constants import STAGING_DIR, DIMENSION_DIR
from pyspark.sql.functions import monotonically_increasing_id as iid


class StageToDimension(BaseOperator):
    """This class loads Dimension tables data from staging tables, it will append data in dimension tables

       Args:
           bucket (str): Bucket in which source data is stored and target data will be written.
           table (str): Name of the table that will be loaded (mandatory if append=False)
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 bucket,
                 table,
                 *args, **kwargs):
        super(StageToDimension, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.table = table

    def __join_load_dimension(self, spark, starting_id, dimension_path, join_keys):
        """When dimensions has data from both CSV and JSON sources, this method is used to join
        and remove duplicates from data before appending to the dimension table."""
        staging_expenses_path = os.path.join(self.bucket, STAGING_DIR, f'expenses_{self.table}')
        staging_vouchers_path = os.path.join(self.bucket, STAGING_DIR, f'vouchers_{self.table}')

        df_expenses = spark.read.parquet(staging_expenses_path).dropna()
        df_vouchers = spark.read.parquet(staging_vouchers_path).dropna()
        df_expenses.alias('a').join(
            df_vouchers.alias('b'),
            join_keys,
            how='full') \
            .selectExpr(*join_keys,
                        *[item for item in df_expenses.columns if item not in df_vouchers.columns],
                        *[item for item in df_vouchers.columns if item not in df_expenses.columns],
                        *list(map(lambda x: f"nvl(a.{x}, b.{x}) as {x}",
                                  [item for item in df_expenses.columns if
                                   item in df_vouchers.columns and item not in join_keys]))) \
            .coalesce(1).select((iid() + starting_id).alias(f"{self.table}_id"), "*") \
            .write.parquet(dimension_path, mode='append')

    def __load_dimension(self, spark, starting_id, dimension_path):
        """Loads dimension data from stage when only data is found in only one source, CSV or JSON"""
        staging_path = os.path.join(self.bucket, STAGING_DIR, f'*{self.table}')
        spark.read.parquet(staging_path).coalesce(1).select((iid() + starting_id).alias(f"{self.table}_id"), "*") \
            .write.parquet(dimension_path, mode='append')

    def execute(self, context):
        """Method called by Airflow Task."""
        dimension_path = os.path.join(self.bucket, DIMENSION_DIR, self.table)
        spark = SparkUtils.create_spark_session()
        try:
            starting_id = spark.read.parquet(dimension_path).groupBy() \
                .max(f"{self.table}_id").first().asDict().get(f'max({self.table}_id)')
        except:
            starting_id = 1
        join_tables = {'vendor': Schemas.vendor_keys,
                       'agency': Schemas.agency_keys}
        if self.table in join_tables:
            self.__join_load_dimension(spark, starting_id, dimension_path, join_tables.get(self.table))
        else:
            self.__load_dimension(spark, starting_id, dimension_path)
