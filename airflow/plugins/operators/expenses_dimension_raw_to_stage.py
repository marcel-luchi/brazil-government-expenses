import os
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import Schemas
from helpers import SparkUtils
from helpers.constants import EXPENSES_DIR, STAGING_DIR, \
    DIMENSION_DIR, RAW_DIR, COMMITMENT_FILENAME, SETTLEMENT_FILENAME, PAYMENT_FILENAME, \
    COMMITMENT_ITEM_FILENAME


class ExpensesDimensionRawToStage(BaseOperator):
    """This class loads dimension tables data from staging tables in Redshift
       The source data for this is Brazil's government Portal da Transparencia files

       Downloaded from: http://transparencia.gov.br/download-de-dados/despesas

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
        super(ExpensesDimensionRawToStage, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.table = table
        self.csv_properties = {'header': True,
                               'encoding': 'UTF-8',
                               'sep': ';'}



    def __stage_dimensions_comm_settl_pay(self, spark, params):
        """Processes dimension data based on commitment, settlement and payment files.
        This is done in order to create a unique dimension for all fact tables."""
        year = params.get('year')
        month = params.get('month')
        commitment_path = os.path.join(self.bucket,
                                       RAW_DIR,
                                       EXPENSES_DIR,
                                       year,
                                       f"{params.get('commitment_file')}{year}{month}*")
        settlement_path = os.path.join(self.bucket,
                                       RAW_DIR,
                                       EXPENSES_DIR,
                                       year,
                                       f'{SETTLEMENT_FILENAME}{year}{month}*')
        payment_path = os.path.join(self.bucket,
                                    RAW_DIR,
                                    EXPENSES_DIR,
                                    year,
                                    f'{PAYMENT_FILENAME}{year}{month}*')
        dimension_path = os.path.join(self.bucket,
                                      DIMENSION_DIR,
                                      params.get('table'))
        staging_path = os.path.join(self.bucket,
                                    STAGING_DIR,
                                    params.get('output_file'))

        self.log.info("Reading input files:")
        self.log.info(commitment_path)
        self.log.info(settlement_path)
        self.log.info(payment_path)
        self.log.info(dimension_path)
        self.log.info("Writing output to:")
        self.log.info(staging_path)

        df_commitments = spark.read.csv(commitment_path,
                                        sep=self.csv_properties.get('sep'),
                                        encoding=self.csv_properties.get('encoding'),
                                        header=self.csv_properties.get('header')
                                        ).selectExpr(params.get('schema'))

        df_settlements = spark.read.csv(settlement_path,
                                        sep=self.csv_properties.get('sep'),
                                        encoding=self.csv_properties.get('encoding'),
                                        header=self.csv_properties.get('header')
                                        ).selectExpr(params.get('schema'))

        df_payments = spark.read.csv(payment_path,
                                     sep=self.csv_properties.get('sep'),
                                     encoding=self.csv_properties.get('encoding'),
                                     header=self.csv_properties.get('header')
                                     ).selectExpr(params.get('schema'))
        try:
            df_dimension = spark.read.parquet(dimension_path)
            self.log.info(f'Dimension found, has {df_dimension.count()} records')

        except:
            df_dimension = None
            self.log.info('Dimension not found.')

        if df_dimension is None:
            df_commitments.unionByName(df_settlements.unionByName(df_payments))\
                .dropDuplicates(params.get('table_key')).dropna(subset=params.get('table_key')) \
                .write.parquet(staging_path, mode='overwrite')

        else:
            df_commitments.unionByName(df_settlements.unionByName(df_payments))\
                .dropDuplicates(params.get('table_key')).dropna(subset=params.get('table_key')) \
                .join(df_dimension, params.get('table_key'), how='leftanti') \
                .write.parquet(staging_path, mode='overwrite')

    def __stage_dimensions_one_source(self, spark, params):
        """Process one file to create a dimension, for data that is only present in one source file."""
        year = params.get('year')
        month = params.get('month')
        path = os.path.join(self.bucket,
                            RAW_DIR,
                            EXPENSES_DIR,
                            year,
                            f"{params.get('file')}{year}{month}*")

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

        df_data = spark.read.csv(path,
                                 sep=self.csv_properties.get('sep'),
                                 encoding=self.csv_properties.get('encoding'),
                                 header=self.csv_properties.get('header')
                                 ).selectExpr(params.get('schema'))

        try:
            df_dimension = spark.read.parquet(dimension_path)
            self.log.info(f'Dimension found, has {df_dimension.count()} records')

        except:
            df_dimension = None
            self.log.info('Dimension not Found.')

        if df_dimension is None:
            df_data.dropDuplicates(params.get('table_key')).dropna(subset=params.get('table_key'))\
                .coalesce(1).write.parquet(staging_path, mode='overwrite')

        else:
            df_data.dropDuplicates(params.get('table_key')).dropna(subset=params.get('table_key')) \
                .join(df_dimension, params.get('table_key'), how='leftanti')\
                .write.parquet(staging_path, mode='overwrite')

    def execute(self, context):
        """Method called by Airflow Task."""
        year = '{execution_date.year}'.format(**context)
        month = '{execution_date.month}'.format(**context).zfill(2)
        spark = SparkUtils.create_spark_session()

        extractions = {'agency':
                           {'params': {'table': self.table,
                                       'output_file': 'expenses_agency',
                                       'year': year,
                                       'month': month,
                                       'schema': Schemas.expenses_agency_columns,
                                       'table_key': Schemas.agency_keys,
                                       'commitment_file': COMMITMENT_FILENAME,
                                       'settlement_file': SETTLEMENT_FILENAME,
                                       'payment_file': PAYMENT_FILENAME},
                            'method': self.__stage_dimensions_comm_settl_pay},
                       'vendor': {'params': {'table': self.table,
                                             'output_file': 'expenses_vendor',
                                             'year': year,
                                             'month': month,
                                             'schema': Schemas.expenses_vendor_columns,
                                             'table_key': Schemas.vendor_keys,
                                             'commitment_file': COMMITMENT_FILENAME,
                                             'settlement_file': SETTLEMENT_FILENAME,
                                             'payment_file': PAYMENT_FILENAME
                                             },
                                  'method': self.__stage_dimensions_comm_settl_pay},
                       'expense_type': {'params': {'table': self.table,
                                                   'output_file': 'expense_type',
                                                   'year': year,
                                                   'month': month,
                                                   'schema': Schemas.expense_type_columns,
                                                   'table_key': Schemas.expense_type_keys,
                                                   'file': COMMITMENT_ITEM_FILENAME
                                                   },
                                        'method': self.__stage_dimensions_one_source}
                       }

        method = extractions.get(self.table).get('method')
        params = extractions.get(self.table).get('params')

        method(spark, params)
