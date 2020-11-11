# brazil-government-credit-card-usage
Build a datalake and a datawarehouse to analyze brazilian government credit card usage

## Installation

Place airflow objects under your airflow directory

AWS utils has functions to create a redshift cluster, and arn roles.
After cluster is created, run *create_tables.sql* in order to create
all needed tables in redshift.

Create the below connections in Airflow:
*aws_credentials*, type aws with key and secret to aws S3

*transparencia_redshift*, type postgres, pointing to your redshift cluster/db.

Create the below variables in Aiflow:
*transparencia_access_key* create a key in portaltransparencia.gov.br 
or use value 6ad76d85e57c0936d116ced2b3211d8c

*transparencia_arn_rule*, arn rule from redshift cluster to s3 bucket.

*transparencia_script_path*, directory where *vouchers_etl.py* is placed

Usage:
Run **Process_Government_Cards_Vouchers** in airflow

task read_vouchers_operator in read_card_data.py has limit_pages parameter set to 5
this limits the reader to limit to 5 pages of data, as a full extraction could take up to 1 hour.
To execute a full extraction, remove limit_pages parameter.