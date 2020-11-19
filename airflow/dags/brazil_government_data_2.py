from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from helpers import RedshiftQueries

from airflow.operators import (TransparenciaApiReaderOperator)
from airflow.operators import (TransparenciaLoadDimensionOperator)
from airflow.operators import (TransparenciaLoadFactOperator)
from airflow.operators import (DataQualityOperator)
from airflow.operators import (TransparenciaRedshiftStageOperator)

script_path = Variable.get("transparencia_script_path")
arn_rule = Variable.get("transparencia_arn_rule")
s3_bucket = '/home/public/brgovdata/raw_data'
default_args = {'start_date': datetime(2014, 1, 1),
                'depends_on_past': False,
                'catchup': True}

dag = DAG('Extract_Government_Data',
          default_args=default_args,
          description="Reads data from Brazil's federal government API and writes to s3_bucket.",
          schedule_interval="0 0 * * *",
          max_active_runs=3
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)
end_operator = DummyOperator(task_id='End_execution', dag=dag)


read_gov_agency = TransparenciaApiReaderOperator(task_id='Read_Government_Agency',
                                                        dag=dag,
                                                        aws_conn_id='aws_credentials',
                                                        storage_type="local",
                                                        extraction_type='government_agencies',
                                                        s3_bucket=s3_bucket,
                                                        access_key="6ad76d85e57c0936d116ced2b3211d8c",
                                                        sleep_time=2,
                                                        provide_context=True
                                                        )

read_agency_expenses = TransparenciaApiReaderOperator(task_id='Read_Agency_Expenses',
                                                        dag=dag,
                                                        aws_conn_id='aws_credentials',
                                                        storage_type="local",
                                                        extraction_type='agency_expenses_documents',
                                                        s3_bucket=s3_bucket,
                                                        access_key="6ad76d85e57c0936d116ced2b3211d8c",
                                                        sleep_time=1,
                                                        provide_context=True)

start_operator >> read_gov_agency
read_gov_agency >> read_agency_expenses
read_agency_expenses >> end_operator