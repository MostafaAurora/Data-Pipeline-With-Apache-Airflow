from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults




class LoadFactOperator(BaseOperator):
    insert_sql = """
        INSERT INTO {}
        {};
    """
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 insert_query='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.insert_query=insert_query


    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        formatted_insert_query = LoadFactOperator.insert_sql.format(self.table, self.insert_query)
        self.log.info(f'Running insert query: \n{formatted_insert_query}')
        redshift.run(formatted_insert_query)
        self.log.info('LoadFactOperator task completed for table: {}'.format(self.table))