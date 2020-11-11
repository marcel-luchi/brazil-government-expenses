from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class TransparenciaRedshiftStageOperator(BaseOperator):
    """Airflow operator to stage data from S3 parquet tables to Redshift staging_tables.

       Args:
           table (str): Target table
           redshift_conn_id (str): Airflow connection to database
           arn_rule (str): Arn rule needed to connect to S3
           query: (str): Query to copy data from S3 to Redshift
           """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table='',
                 redshift_conn_id='redshift_transparencia',
                 arn_rule='',
                 query='',
                 *args, **kwargs):
        super(TransparenciaRedshiftStageOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.arn_rule = arn_rule

    def execute(self, context):
        """Method called by Airflow Task."""
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Starting to load table{self.table}")
        redshift.run(f"truncate table {self.table};")
        redshift.run(self.query.format(arn_rule=self.arn_rule))
        self.log.info(f"Finished to load table{self.table}")
