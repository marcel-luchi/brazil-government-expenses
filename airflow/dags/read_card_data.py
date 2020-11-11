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
s3_bucket = 'brgovdata'
default_args = {'start_date': datetime(2020, 9, 1),
                'depends_on_past': False,
                'catchup': True}

dag = DAG('Process_Government_Cards_Vouchers',
          default_args=default_args,
          description="Reads data from Brazil's federal government corporative card API and writes to s3_bucket.",
          schedule_interval="0 0 6 * *",
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)
end_analytic_tables_operator = DummyOperator(task_id='End_Analytics_Tables', dag=dag)
end_operator = DummyOperator(task_id='End_execution', dag=dag)


read_vouchers_operator = TransparenciaApiReaderOperator(task_id='Read_Data_From_API',
                                                        dag=dag,
                                                        base_url="http://www.portaltransparencia.gov.br/api-de-dados",
                                                        aws_conn_id='aws_credentials',
                                                        api_endpoint="/cartoes",
                                                        s3_bucket=s3_bucket,
                                                        filename='raw_data/vouchers_{year}_{month}.json',
                                                        pagina=1,
                                                        access_key="6ad76d85e57c0936d116ced2b3211d8c",
                                                        provide_context=True,
                                                        limit_pages=5
                                                        )

process_fact_vouchers = BashOperator(
   task_id='fact_pagamentos',
   bash_command=f'python {script_path}/vouchers_etl.py pagamentos {s3_bucket} {{{{ execution_date.year }}}} {{{{ execution_date.month }}}} {script_path}',
   dag=dag
)
process_dim_cartao_funcionario = BashOperator(
   task_id='dim_cartao_funcionarios',
   bash_command=f'python {script_path}/vouchers_etl.py cartaofuncionario {s3_bucket} {{{{ execution_date.year }}}} {{{{ execution_date.month }}}} {script_path}',
   dag=dag
)
process_dim_estabelecimentos = BashOperator(
   task_id='dim_estabelecimentos',
   bash_command=f'python {script_path}/vouchers_etl.py estabelecimento {s3_bucket} {{{{ execution_date.year }}}} {{{{ execution_date.month }}}} {script_path}',
   dag=dag
)
process_dim_unidade_gestora = BashOperator(
   task_id='dim_unidade_gestora',
   bash_command=f'python {script_path}/vouchers_etl.py unidadegestora {s3_bucket} {{{{ execution_date.year }}}} {{{{ execution_date.month }}}} {script_path}',
   dag=dag
)

process_dim_municipio = BashOperator(
   task_id='dim_municipio',
   bash_command=f'python {script_path}/vouchers_etl.py municipio {s3_bucket} {{{{ execution_date.year }}}} {{{{ execution_date.month }}}} {script_path}',
   dag=dag
)

stage_fact_pagamentos_operator = TransparenciaRedshiftStageOperator(
   task_id='stage_redshift_pagamentos_fact',
   table='stage_ft_pagamentos',
   redshift_conn_id="transparencia_redshift",
   arn_rule=arn_rule,
   query=RedshiftQueries.copy_pagamentos,
   dag=dag
)

stage_dim_municipio = TransparenciaRedshiftStageOperator(
   task_id='stage_redshift_municipio_dimension',
   table='stage_dm_municipio',
   redshift_conn_id="transparencia_redshift",
   arn_rule=arn_rule,
   query=RedshiftQueries.copy_municipio,
   dag=dag
)

stage_dim_cartao = TransparenciaRedshiftStageOperator(
   task_id='stage_redshift_cartao_funcionario_dimension',
   table='stage_dm_cartao_funcionario',
   redshift_conn_id="transparencia_redshift",
   arn_rule=arn_rule,
   query=RedshiftQueries.copy_cartao,
   dag=dag
)

stage_dim_unidade = TransparenciaRedshiftStageOperator(
   task_id='stage_redshift_unidade_gestora_dimension',
   table='stage_dm_unidade_gestora',
   redshift_conn_id="transparencia_redshift",
   arn_rule=arn_rule,
   query=RedshiftQueries.copy_unidade,
   dag=dag
)

stage_dim_estabelecimento = TransparenciaRedshiftStageOperator(
   task_id='stage_redshift_estabelecimento_dimension',
   table='stage_dm_estabelecimento',
   redshift_conn_id="transparencia_redshift",
   arn_rule=arn_rule,
   query=RedshiftQueries.copy_estabelecimento,
   dag=dag
)

load_fact_pagamentos = TransparenciaLoadFactOperator(
    task_id='load_fact_pagamentos',
    redshift_conn_id="transparencia_redshift",
    query=RedshiftQueries.insert_pagamentos,
    dag=dag
)

load_dim_municipio = TransparenciaLoadDimensionOperator(
    task_id='load_dim_municipio',
    redshift_conn_id="transparencia_redshift",
    append=True,
    query=RedshiftQueries.insert_municipio,
    dag=dag
)

load_dim_cartao = TransparenciaLoadDimensionOperator(
    task_id='load_dim_cartao',
    redshift_conn_id="transparencia_redshift",
    append=True,
    query=RedshiftQueries.insert_cartao,
    dag=dag
)

load_dim_unidade = TransparenciaLoadDimensionOperator(
    task_id='load_dim_unidade',
    redshift_conn_id="transparencia_redshift",
    append=True,
    query=RedshiftQueries.insert_unidade,
    dag=dag
)

load_dim_estabelecimento = TransparenciaLoadDimensionOperator(
    task_id='load_dim_estabelecimento',
    redshift_conn_id="transparencia_redshift",
    append=True,
    query=RedshiftQueries.insert_estabelecimento,
    dag=dag
)

data_quality_operator = DataQualityOperator(
   task_id='check_data_quality',
   tables=['dm_municipio', 'dm_unidade_gestora', 'dm_estabelecimento',
           'dm_cartao_funcionario', 'ft_pagamentos'],
   redshift_conn_id="transparencia_redshift",
   validation_threshold=1,
   dag=dag
)


start_operator >> read_vouchers_operator

read_vouchers_operator >> process_fact_vouchers
read_vouchers_operator >> process_dim_cartao_funcionario
read_vouchers_operator >> process_dim_estabelecimentos
read_vouchers_operator >> process_dim_unidade_gestora
read_vouchers_operator >> process_dim_municipio

process_fact_vouchers >> end_analytic_tables_operator
process_dim_cartao_funcionario >> end_analytic_tables_operator
process_dim_estabelecimentos >> end_analytic_tables_operator
process_dim_unidade_gestora >> end_analytic_tables_operator
process_dim_municipio >> end_analytic_tables_operator

end_analytic_tables_operator >> stage_fact_pagamentos_operator
end_analytic_tables_operator >> stage_dim_municipio
end_analytic_tables_operator >> stage_dim_cartao
end_analytic_tables_operator >> stage_dim_unidade
end_analytic_tables_operator >> stage_dim_estabelecimento


stage_fact_pagamentos_operator >> load_fact_pagamentos
stage_dim_estabelecimento >> load_dim_estabelecimento
stage_dim_cartao >> load_dim_cartao
stage_dim_unidade >> load_dim_unidade
stage_dim_municipio >> load_dim_municipio

load_fact_pagamentos >> data_quality_operator
load_dim_estabelecimento >> data_quality_operator
load_dim_unidade >> data_quality_operator
load_dim_cartao >> data_quality_operator
load_dim_municipio >> data_quality_operator

data_quality_operator >> end_operator
