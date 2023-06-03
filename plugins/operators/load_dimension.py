from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults




class LoadDimensionOperator(BaseOperator):
    insert_sql = """
        INSERT INTO {}
        {};
    """
    
    clear_table_sql = """
        DELETE FROM {};
    """
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 insert_query='',
                 clear_table=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.insert_query=insert_query
        self.clear_table=clear_table


    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.clear_table:
            self.log.info('Clearing data from Redshift dimension table {}'.format(self.table))
            clear_table_query = LoadDimensionOperator.clear_table_sql.format(self.table)
            redshift.run(clear_table_query)
        
        formatted_insert_query = LoadDimensionOperator.insert_sql.format(self.table, self.insert_query)
        self.log.info(f'Running insert query: \n{formatted_insert_query}')
        redshift.run(formatted_insert_query)
        self.log.info('LoadDimensionOperator task completed for table: {}'.format(self.table))