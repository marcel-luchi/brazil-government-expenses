from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

from airflow.operators import (ExpensesDimensionRawToStage)
from airflow.operators import (StageToDimension)
from airflow.operators import (VouchersDimensionRawToStage)
from airflow.operators import (ExpensesFactFromRaw)
from airflow.operators import (VouchersFactFromRaw)
from airflow.operators import (DataQualityOperator)
from airflow.operators import (TransparenciaApiReaderOperator)

from helpers.schemas import Schemas

bucket = Variable.get("transparencia_bucket")
access_key = Variable.get("transparencia_access_key")

default_args = {'start_date': datetime(2020, 1, 1),
                'end_date': datetime(2020, 1, 31),
                'depends_on_past': True,
                'catchup': True,
                'retries': 5,
                'retry_delay': timedelta(minutes=2)}

dag = DAG('Extract_Government_Data',
          default_args=default_args,
          description="Reads data from Brazil's federal government API and writes to s3_bucket.",
          schedule_interval="0 0 20 * *",
          max_active_runs=1,

          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)
end_extraction_operator = DummyOperator(task_id='End_extraction', dag=dag)
end_staging_operator = DummyOperator(task_id='End_staging', dag=dag)
end_dimension_operator = DummyOperator(task_id='End_dimension', dag=dag)
end_operator = DummyOperator(task_id='End_execution', dag=dag)

extract_credit_card_vouchers = TransparenciaApiReaderOperator(task_id='Extract_Credit_Card_Vouchers',
                                                              dag=dag,
                                                              storage_type='local',
                                                              aws_conn_id='aws_credentials',
                                                              extraction_type='corporate_card_expenses',
                                                              s3_bucket=bucket,
                                                              access_key=access_key)

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
data_quality_agency = DataQualityOperator(task_id='Data_Quality_Agency',
                                          dag = dag,
                                          bucket = bucket,
                                          table='agency',
                                          validation_type='dimension_duplicate_keys',
                                          keys_list = Schemas.agency_keys)

load_dim_vendor = StageToDimension(task_id='Load_Dim_Vendor',
                                   dag=dag,
                                   bucket=bucket,
                                   table='vendor')

data_quality_vendor = DataQualityOperator(task_id='Data_Quality_Vendor',
                                          dag = dag,
                                          bucket = bucket,
                                          table='vendor',
                                          validation_type='dimension_duplicate_keys',
                                          keys_list = Schemas.vendor_keys)

load_dim_expense_type = StageToDimension(task_id='Load_Dim_Expense_Type',
                                   dag=dag,
                                   bucket=bucket,
                                   table='expense_type')
data_quality_expense_type = DataQualityOperator(task_id='Data_Quality_Expense_Type',
                                          dag = dag,
                                          bucket = bucket,
                                          table='expense_type',
                                          validation_type='dimension_duplicate_keys',
                                          keys_list = Schemas.expense_type_keys)

load_dim_city = StageToDimension(task_id='Load_Dim_City',
                                   dag=dag,
                                   bucket=bucket,
                                   table='city')
data_quality_city = DataQualityOperator(task_id='Data_Quality_City',
                                          dag = dag,
                                          bucket = bucket,
                                          table='city',
                                          validation_type='dimension_duplicate_keys',
                                          keys_list = Schemas.city_keys)

load_fact_commitment = ExpensesFactFromRaw(task_id = 'Load_Fact_Commitment',
                                              dag=dag,
                                              bucket=bucket,
                                              table='commitment')
data_quality_fact_commitment = DataQualityOperator(task_id='Data_Quality_Commitment',
                                          dag = dag,
                                          bucket = bucket,
                                          table='commitment',
                                          validation_type='fact_keys',
                                          validation_threshold = 2,
                                          keys_list = ['commitment_id', 'vendor_id', 'agency_id'])

load_fact_payment = ExpensesFactFromRaw(task_id = 'Load_Fact_Payment',
                                              dag=dag,
                                              bucket=bucket,
                                              table='payment')
data_quality_fact_payment = DataQualityOperator(task_id='Data_Quality_Payment',
                                          dag = dag,
                                          bucket = bucket,
                                          table='payment',
                                          validation_type='fact_keys',
                                          validation_threshold = 2,
                                          keys_list = ['payment_code', 'vendor_id', 'agency_id'])

load_fact_voucher_payment = VouchersFactFromRaw(task_id='Load_Fact_Vouchers_Payment',
                                               dag=dag,
                                               bucket=bucket,
                                               table='voucher_payment'
                                               )
data_quality_fact_voucher_payment = DataQualityOperator(task_id='Data_Quality_Voucher_Payment',
                                          dag = dag,
                                          bucket = bucket,
                                          table='voucher_payment',
                                          validation_type='fact_keys',
                                          validation_threshold = 2,
                                          keys_list = ['payment_id', 'vendor_id', 'agency_id',
                                          'city_id'])

start_operator >> extract_credit_card_vouchers

extract_credit_card_vouchers >> end_extraction_operator

end_extraction_operator >> read_gov_agency
end_extraction_operator >> read_vendors
end_extraction_operator >> read_expense_type
end_extraction_operator >> read_voucher_vendors
end_extraction_operator >> read_voucher_agencies
end_extraction_operator >> read_voucher_cities

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


load_dim_vendor >> data_quality_vendor
load_dim_agency >> data_quality_agency
load_dim_expense_type >> data_quality_expense_type
load_dim_city >> data_quality_city

data_quality_vendor >> end_dimension_operator
data_quality_agency >> end_dimension_operator
data_quality_expense_type >> end_dimension_operator
data_quality_city >> end_dimension_operator

end_dimension_operator >> load_fact_commitment
end_dimension_operator >> load_fact_payment
end_dimension_operator >> load_fact_voucher_payment

load_fact_commitment >> data_quality_fact_commitment
load_fact_payment >> data_quality_fact_payment
load_fact_voucher_payment >> data_quality_fact_voucher_payment

data_quality_fact_commitment >> end_operator
data_quality_fact_payment >> end_operator
data_quality_fact_voucher_payment >> end_operator