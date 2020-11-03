import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (TransparenciaApiReaderOperator)

default_args = {'start_date': datetime(2020, 11, 1)}

dag = DAG('Read_Government_People_Data',
          default_args=default_args,
          description="Reads data from Brazil's federal government People and writes to s3_bucket.",
          schedule_interval="0 0 28 * *",
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

read_organ_operator = TransparenciaApiReaderOperator(task_id='Read_Organ_Data_From_API',
                                                     dag=dag,
                                                     base_url="http://www.portaltransparencia.gov.br/api-de-dados",
                                                     api_endpoint="/orgaos-siape",
                                                     s3_bucket='/home/public/orgaos_siape',
                                                     filename='orgaos_{year}_{month}.json',
                                                     pagina=1,
                                                     access_key="6ad76d85e57c0936d116ced2b3211d8c",
                                                     provide_context=True
                                                     )

read_people_operator = TransparenciaApiReaderOperator(task_id='Read_People_Data_From_API',
                                                      dag=dag,
                                                      api_endpoint="/servidores",
                                                      base_url="http://www.portaltransparencia.gov.br/api-de-dados",
                                                      s3_bucket='/home/public/servidores',
                                                      filename='servidores_{year}_{month}.json',
                                                      pagina=1,
                                                      access_key="6ad76d85e57c0936d116ced2b3211d8c",
                                                      provide_context=True,
                                                      dependency_bucket='/home/public/orgaos_siape',
                                                      dependency_file='orgaos_{year}_{month}.json',
                                                      dependency_params=[("codigo", "orgaoServidorExercicio")]
                                                      )

start_operator >> read_organ_operator
read_organ_operator >> read_people_operator
