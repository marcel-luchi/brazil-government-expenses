import os
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.spark_utils import SparkUtils
from pyspark.sql.functions import broadcast
from helpers import Schemas
from helpers.constants import RAW_DIR, FACT_DIR, VOUCHERS_FILENAME, VOUCHERS_DIR


class VouchersFactFromRaw(BaseOperator):
    """This class loads fact tables data from credit card vouchers data to fact tables
       The source data for this is Json files extracted from Brazil's government Portal da Transparencia API.
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
        super(VouchersFactFromRaw, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.table = table
        self.csv_properties = {'header': True,
                               'encoding': 'ISO-8859-1',
                               'sep': ';'}

    def __process_fact_vendor_agency_city(self, spark, params):
        "Process fact tables retrieving dimension ids from vendor, agency and city tables."
        year = params.get('year')
        month = params.get('month')
        path = os.path.join(self.bucket,
                            RAW_DIR,
                            VOUCHERS_DIR,
                            f"{params.get('file')}{year}-{month}*")
        fact_path = os.path.join(self.bucket,
                                 FACT_DIR,
                                 self.table)
        df_agencies = spark.read.parquet('/home/public/brgovdata/dimension/agency/').select(*Schemas.agency_keys,
                                                                                            'agency_id')
        df_vendors = spark.read.parquet('/home/public/brgovdata/dimension/vendor/').select(*Schemas.vendor_keys,
                                                                                           "vendor_id")
        df_cities = spark.read.parquet('/home/public/brgovdata/dimension/city/').select(*Schemas.city_keys,
                                                                                          "city_id")
        spark.read.json(path,
                        schema=Schemas.voucher_schema).selectExpr(*params.get('schema'),
                                                                  f"{year} as year",
                                                                  f"{month} as month"
                                                                  ) \
            .join(broadcast(df_vendors), Schemas.vendor_keys, how='left') \
            .join(broadcast(df_agencies), Schemas.agency_keys, how='left') \
            .join(broadcast(df_cities), Schemas.city_keys, how='left') \
            .drop("vendor_code",
                  "vendor_name",
                  "branch_code",
                  "agency_code",
                  "agency",
                  "highest_department_code",
                  "city_code",
                  "city",
                  "country",
                  "state",
                  "state_acronym") \
            .write.partitionBy("year", "month").parquet(fact_path, mode='overwrite')

    def execute(self, context):
        """Method called by Airflow Task."""
        year = '{execution_date.year}'.format(**context)
        month = '{execution_date.month}'.format(**context).zfill(2)
        spark = SparkUtils.create_spark_session()
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        extractions = {'voucher_payment':
                           {'params': {'table': self.table,
                                       'year': year,
                                       'month': month,
                                       'schema': Schemas.voucher_payment_fact_columns,
                                       'file': VOUCHERS_FILENAME},
                            'method': self.__process_fact_vendor_agency_city}

                       }

        method = extractions.get(self.table).get('method')
        params = extractions.get(self.table).get('params')

        method(spark, params)
