from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

from airflow.operators import (ExpensesDimensionRawToStage)
from airflow.operators import (StageToDimension)
from airflow.operators import (VouchersDimensionRawToStage)
from airflow.operators import (ExpensesFactFromRaw)

script_path = Variable.get("transparencia_script_path")
arn_rule = Variable.get("transparencia_arn_rule")
bucket = '/home/public/brgovdata'
access_key = Variable.get("transparencia_access_key")

default_args = {'start_date': datetime(2015, 1, 20),
                'end_date': datetime(2015, 6, 30),
                'depends_on_past': False,
                'catchup': True,
                'retries': 5,
                'retry_delay': timedelta(minutes=2)}

dag = DAG('Extract_Government_Data',
          default_args=default_args,
          description="Reads data from Brazil's federal government API and writes to s3_bucket.",
          schedule_interval="0 0 20 * *",
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)
end_operator = DummyOperator(task_id='End_execution', dag=dag)
end_staging_operator = DummyOperator(task_id='End_Staging', dag=dag)
end_dimension_operator = DummyOperator(task_id='End_Dimension', dag=dag)


read_gov_agency = ExpensesDimensionRawToStage(task_id='Stage_Agencies',
                                                        dag=dag,
                                                        bucket=bucket,
                                                        table='agency',
                                                        provide_context=True
                                                        )

read_vendors = ExpensesDimensionRawToStage(task_id='Stage_Vendors',
                                                        dag=dag,
                                                        bucket=bucket,
                                                        table='vendor',
                                                        provide_context=True
                                                        )
read_expense_type = ExpensesDimensionRawToStage(task_id='Stage_Expense_Type',
                                                        dag=dag,
                                                        bucket=bucket,
                                                        table='expense_type',
                                                        provide_context=True
                                                        )

read_voucher_vendors = VouchersDimensionRawToStage(task_id='Stage_Vouchers_Vendors',
                                                        dag=dag,
                                                        bucket=bucket,
                                                        table='vendor',
                                                        provide_context=True
                                                        )

read_voucher_agencies = VouchersDimensionRawToStage(task_id='Stage_Vouchers_Agencies',
                                                        dag=dag,
                                                        bucket=bucket,
                                                        table='agency',
                                                        provide_context=True
                                                        )

read_voucher_cities = VouchersDimensionRawToStage(task_id='Stage_Vouchers_Cities',
                                                        dag=dag,
                                                        bucket=bucket,
                                                        table='city',
                                                        provide_context=True
                                                        )

load_dim_agency = StageToDimension(task_id='Load_Dim_Agencies',
                                   dag=dag,
                                   bucket=bucket,
                                   table='agency')

load_dim_vendor = StageToDimension(task_id='Load_Dim_Vendor',
                                   dag=dag,
                                   bucket=bucket,
                                   table='vendor')

load_dim_expense_type = StageToDimension(task_id='Load_Dim_Expense_Type',
                                   dag=dag,
                                   bucket=bucket,
                                   table='expense_type')

load_dim_city = StageToDimension(task_id='Load_Dim_City',
                                   dag=dag,
                                   bucket=bucket,
                                   table='city')

load_fact_commitment = ExpensesFactFromRaw(task_id = 'Load_Fact_Commitment',
                                              dag=dag,
                                              bucket=bucket,
                                              table='commitment')

start_operator >> read_gov_agency
start_operator >> read_vendors
start_operator >> read_expense_type
start_operator >> read_voucher_vendors
start_operator >> read_voucher_agencies

read_gov_agency >> end_staging_operator
read_vendors >> end_staging_operator
read_expense_type >> end_staging_operator
read_voucher_vendors >> end_staging_operator
read_voucher_agencies >> end_staging_operator
read_voucher_cities >> end_staging_operator

end_staging_operator >> load_dim_agency
end_staging_operator >> load_dim_vendor
end_staging_operator >> load_dim_expense_type
end_staging_operator >> load_dim_city


load_dim_vendor >> end_dimension_operator
load_dim_agency >> end_dimension_operator
load_dim_expense_type >> end_dimension_operator
load_dim_city >> end_dimension_operator

end_dimension_operator >> load_fact_commitment

load_fact_commitment >> end_operator