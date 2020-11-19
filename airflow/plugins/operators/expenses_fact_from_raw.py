import os
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from helpers import Schemas
from helpers.constants import EXPENSES_DIR, RAW_DIR, FACT_DIR, COMMITMENT_FILENAME


class ExpensesFactFromRaw(BaseOperator):
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
        super(ExpensesFactFromRaw, self).__init__(*args, **kwargs)
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
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        return spark

    def __process_fact_commitment(self, spark, params):
        year = params.get('year')
        month = params.get('month')
        path = os.path.join(self.bucket,
                            RAW_DIR,
                            EXPENSES_DIR,
                            year,
                            f"{params.get('commitment_file')}{year}{month}*")
        fact_path = os.path.join(self.bucket,
                                 FACT_DIR,
                                 self.table)
        df_agencies = spark.read.parquet('/home/public/brgovdata/dimension/agency/')
        df_vendors = spark.read.parquet('/home/public/brgovdata/dimension/vendor/').select("vendor_id", "vendor_code",
                                                                                           "vendor_name")
        spark.read.csv(path,
                       sep=self.csv_properties.get('sep'),
                       encoding=self.csv_properties.get('encoding'),
                       header=self.csv_properties.get('header')
                       ).selectExpr(params.get('schema')) \
            .join(broadcast(df_vendors), Schemas.vendor_keys, how='left') \
            .join(broadcast(df_agencies), Schemas.agency_keys,how='left') \
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
        spark = self.__create_spark_session()

        extractions = {'commitment':
                           {'params': {'table': self.table,
                                       'output_file': 'fact_commitment',
                                       'year': year,
                                       'month': month,
                                       'schema': Schemas.commitment_fact_columns,
                                       'commitment_file': COMMITMENT_FILENAME},
                            'method': self.__process_fact_commitment}

                       }

        method = extractions.get(self.table).get('method')
        params = extractions.get(self.table).get('params')

        method(spark, params)
