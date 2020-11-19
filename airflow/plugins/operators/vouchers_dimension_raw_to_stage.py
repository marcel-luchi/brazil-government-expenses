import os
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from pyspark.sql import SparkSession
from helpers import Schemas
from helpers.constants import VOUCHERS_DIR, STAGING_DIR, \
    DIMENSION_DIR, RAW_DIR, VOUCHERS_FILENAME


class VouchersDimensionRawToStage(BaseOperator):
    """This class loads Dimension tables data from staging tables in Redshift

       Args:
           table (str): Name of the table that will be loaded (mandatory if append=False)
           redshift_conn_id (str): Airflow connection to database
           query (str): Query that loads the data from staging to final table
           append (bool): If True, appends data to the table, otherwise data will be overwriten
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 bucket,
                 table,
                 *args, **kwargs):
        super(VouchersDimensionRawToStage, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.table = table
        self.csv_properties = {'header': True,
                               'encoding': 'ISO-8859-1',
                               'sep': ';'}

    def __create_spark_session(self):
        spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .getOrCreate()
        spark.conf.set("spark.sql.shuffle.partitions", '4')
        return spark

    def __stage_dimensions(self, spark, params):
        year = params.get('year')
        month = params.get('month')
        path = os.path.join(self.bucket,
                            RAW_DIR,
                            VOUCHERS_DIR,
                            f"{params.get('file')}{year}-{month}*")

        dimension_path = os.path.join(self.bucket,
                                      DIMENSION_DIR,
                                      params.get('table'))
        staging_path = os.path.join(self.bucket,
                                    STAGING_DIR,
                                    params.get('output_file'))

        self.log.info("Reading input files:")
        self.log.info(path)
        self.log.info("Writing output to:")
        self.log.info(staging_path)

        df_data = spark.read.json(path, schema=Schemas.voucher_schema).selectExpr(params.get('schema'))

        try:
            df_dimension = spark.read.parquet(dimension_path)

        except:
            df_dimension = None

        if df_dimension is None:
            df_data.dropDuplicates().write.parquet(staging_path, mode='overwrite')

        else:
            df_data.dropDuplicates() \
                .join(df_dimension, params.get('table_key'), how='leftanti')\
                .write.parquet(staging_path, mode='overwrite')

    def execute(self, context):
        """Method called by Airflow Task."""
        year = '{execution_date.year}'.format(**context)
        month = '{execution_date.month}'.format(**context).zfill(2)
        spark = self.__create_spark_session()
        extractions = {'vendor': {'params': {'table': self.table,
                                             'output_file': 'vouchers_vendor',
                                             'year': year,
                                             'month': month,
                                             'schema': Schemas.voucher_vendor_columns,
                                             'table_key': Schemas.vendor_keys,
                                             'file': VOUCHERS_FILENAME
                                             }},
                       'agency': {'params': {'table': self.table,
                                             'output_file': 'vouchers_agency',
                                             'year': year,
                                             'month': month,
                                             'schema': Schemas.voucher_agency_columns,
                                             'table_key': Schemas.agency_keys,
                                             'file': VOUCHERS_FILENAME
                                             }},
                       'city': {'params': {'table': self.table,
                                           'output_file': 'vouchers_city',
                                           'year': year,
                                           'month': month,
                                           'schema': Schemas.voucher_city_columns,
                                           'table_key': Schemas.city_keys,
                                           'file': VOUCHERS_FILENAME
                                           }}
                       }

        params = extractions.get(self.table).get('params')
        self.__stage_dimensions(spark, params)
