from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook




class StageToRedshiftOperator(BaseOperator):
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        {};
    """
    
    clear_table_sql = """
        DELETE FROM {};
    """


    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_path='',
                 s3_region='',
                 additional_params='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_path=s3_path
        self.s3_region=s3_region
        self.additional_params=additional_params
        

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Clearing data from dimension table: {}'.format(self.table))
        clear_data_query = StageToRedshiftOperator.clear_table_sql.format(self.table)
        redshift.run(clear_data_query)
        
        self.log.info('Copying {} table data from S3 to Redshift.'.format(self.table))
        s3_path = 's3://{}/{}'.format(self.s3_bucket, self.s3_path)
        formatted_copy_query = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.s3_region,
            self.additional_params
        )
        self.log.info(f'Running query: \n{formatted_copy_query}')
        redshift.run(formatted_copy_query)
        self.log.info('StageToRedshiftOperator task completed for table {}'.format(self.table))