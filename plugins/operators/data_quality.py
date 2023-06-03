from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 data_quality_checklist=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.data_quality_checklist=data_quality_checklist


    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if len(self.data_quality_checklist) <= 0:
            self.log.info('No quality checks specified. Quality checks canceled.')
            return
        
        for check in self.data_quality_checklist:
            sql_query = check.get('sql_query')
            expected_result = check.get('pass_result')
            
            try:
                self.log.info('Initiating query for data check - {}'.format(sql_query))
                records = redshift.get_records(sql_query)
                num_records = len(records[0][0])
                
                if num_records != expected_result:
                    raise ValueError('Quality check failed. expected number of entries value is {} and {} was given'.format(expected_result, num_records))
                    
            except ValueError as v:
                self.log.info(v.args)
                raise
            except Exception as e:
                self.log.info('Query for data check failed - {}. Exception: {}'.format(sql_query, e))
                continue