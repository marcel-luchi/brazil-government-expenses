from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (TransparenciaApiReaderOperator)

default_args = {'start_date': datetime(2014, 1, 1)}

dag = DAG('Read_Government_Cards_Vouchers',
          default_args=default_args,
          description="Reads data from Brazil's federal government corporative card API and writes to s3_bucket.",
          schedule_interval="0 0 28 * *",
          max_active_runs=2
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

read_vouchers_operator = TransparenciaApiReaderOperator(task_id='Read_Data_From_API',
                                              dag=dag,
                                              base_url="http://www.portaltransparencia.gov.br/api-de-dados",
                                              api_endpoint="/cartoes",
                                              s3_bucket='/home/public/dados_uso_cartao',
                                              filename='vouchers_{year}_{month}.json',
                                              pagina=1,
                                              access_key="6ad76d85e57c0936d116ced2b3211d8c",
                                              provide_context=True
                                              )

start_operator >> read_vouchers_operator
