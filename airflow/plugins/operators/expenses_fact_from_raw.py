import os
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SparkUtils
from pyspark.sql.functions import broadcast
from helpers import Schemas
from helpers.constants import EXPENSES_DIR, RAW_DIR, FACT_DIR, PAYMENT_FILENAME, COMMITMENT_FILENAME


class ExpensesFactFromRaw(BaseOperator):
    """This class loads fact tables data from staging tables in Redshift
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
        super(ExpensesFactFromRaw, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.table = table
        self.csv_properties = {'header': True,
                               'encoding': 'UTF-8',
                               'sep': ';'}


    def __process_fact_vendor_agency(self, spark, params):
        year = params.get('year')
        month = params.get('month')
        path = os.path.join(self.bucket,
                            RAW_DIR,
                            EXPENSES_DIR,
                            year,
                            f"{params.get('file')}{year}{month}*")
        fact_path = os.path.join(self.bucket,
                                 FACT_DIR,
                                 self.table)
        df_agencies = spark.read.parquet('/home/public/brgovdata/dimension/agency/').select(*Schemas.agency_keys,
                                                                                            'agency_id')
        df_vendors = spark.read.parquet('/home/public/brgovdata/dimension/vendor/').select(*Schemas.vendor_keys,
                                                                                           "vendor_id")
        spark.read.csv(path,
                       sep=self.csv_properties.get('sep'),
                       encoding=self.csv_properties.get('encoding'),
                       header=self.csv_properties.get('header')
                       ).selectExpr(params.get('schema')) \
            .join(broadcast(df_vendors), Schemas.vendor_keys, how='left') \
            .join(broadcast(df_agencies), Schemas.agency_keys, how='left') \
            .drop("vendor_code",
                  "vendor_name",
                  "branch_code",
                  "agency_code",
                  "agency",
                  "highest_department_code") \
            .write.partitionBy("year", "month").parquet(fact_path, mode='overwrite')


    def execute(self, context):
        """Method called by Airflow Task."""
        year = '{execution_date.year}'.format(**context)
        month = '{execution_date.month}'.format(**context).zfill(2)
        spark = SparkUtils.create_spark_session()
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        extractions = {'commitment':
                           {'params': {'table': self.table,
                                       'year': year,
                                       'month': month,
                                       'schema': Schemas.commitment_fact_columns,
                                       'file': COMMITMENT_FILENAME},
                            'method': self.__process_fact_vendor_agency},
                       'payment':
                           {'params': {'table': self.table,
                                       'year': year,
                                       'month': month,
                                       'schema': Schemas.payment_fact_columns,
                                       'file': PAYMENT_FILENAME},
                            'method': self.__process_fact_vendor_agency}

                       }

        method = extractions.get(self.table).get('method')
        params = extractions.get(self.table).get('params')

        method(spark, params)
