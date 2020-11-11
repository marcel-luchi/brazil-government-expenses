from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class TransparenciaLoadFactOperator(BaseOperator):
    """This class loads Fact tables data from staging tables in Redshift

           Args:
               redshift_conn_id (str): Airflow connection to database
               query (str): Query that loads the data from staging to final table
        """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift_transparencia',
                 query='',
                 *args, **kwargs):
        super(TransparenciaLoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query = query

    def execute(self, context):
        """Method called by Airflow Task."""
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(self.query)
