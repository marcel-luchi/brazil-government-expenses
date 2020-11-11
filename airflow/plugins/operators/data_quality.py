from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """Airflow operator for data quality validations

    Args:
        tables (list): list of tables that will be validated for having data.
        redshift_conn_id (str): name of airflow connection that will be used
        validation_threshold (int): minimum number of records for a table to be considered valid
        """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 tables=[],
                 redshift_conn_id='redshift',
                 validation_threshold=1,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id
        self.validation_threshold = validation_threshold

    def execute(self, context):
        """Method called by Airflow Task."""
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('DataQualityOperator not implemented yet')
        for table in self.tables:
            result = redshift.get_records(f"select count(1) from {table}")
            if len(result) < 1 or len(result[0]) < 1 or result[0][0] < self.validation_threshold:
                self.log.error(f"Table {table} has returned no rows.")
                raise KeyError(f"Data quality error. Table {table} returned less records then the threshold\
                                 {self.validation_threshold} records.")
            self.log.info(f"Table {table} validated. Table has {result[0][0]} rows.")
