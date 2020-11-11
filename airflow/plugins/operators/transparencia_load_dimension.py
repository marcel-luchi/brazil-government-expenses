from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class TransparenciaLoadDimensionOperator(BaseOperator):
    """This class loads Dimension tables data from staging tables in Redshift

       Args:
           table (str): Name of the table that will be loaded (mandatory if append=False)
           redshift_conn_id (str): Airflow connection to database
           query (str): Query that loads the data from staging to final table
           append (bool): If True, appends data to the table, otherwise data will be overwriten
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table='',
                 redshift_conn_id='redshift_transparencia',
                 query='',
                 append=True,
                 *args, **kwargs):

        super(TransparenciaLoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.truncate_table = not append

    def execute(self, context):
        """Method called by Airflow Task."""
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate_table:
            self.log.info(f"Table {self.table} will be truncated.")
            redshift.run(f"truncate table {self.table};")
        self.log.info(f"Starting to load table{self.table}")
        redshift.run(self.query)
        self.log.info(f"Finished to load table{self.table}")
