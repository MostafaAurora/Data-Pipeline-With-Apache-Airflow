from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries




default_args = {
    'owner': '',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email': ['mostafa@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}




dag = DAG('ETL_DAG',
          default_args=default_args,
          description='Load and transform data in AWS Redshift with Apache Airflow',
          schedule_interval=timedelta(hours=1)
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)




stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket='<your-bucket-name>',
    s3_path='log_data',
    s3_region='us-west-2',
    additional_params="FORMAT AS JSON 's3://<your-bucket-name>/log_json_path.json'"
)




stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_bucket='<your-bucket-name>',
    s3_path='song_data',
    s3_region='us-west-2',
    additional_params="FORMAT JSON AS 'auto' COMPUPDATE OFF"
)




load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    insert_sql_query=SqlQueries.songplay_table_insert
    
)




load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dimension_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='users',
    insert_sql_query=SqlQueries.user_table_insert,
    clear_table=True
)




load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dimension_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    insert_sql_query=SqlQueries.song_table_insert,
    clear_table=True
)




load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dimension_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    insert_sql_query=SqlQueries.artist_table_insert,
    clear_table=True
)




load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dimension_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    insert_sql_query=SqlQueries.time_table_insert,
    clear_table=True
)




run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    data_quality_checklist=[
        {'sql_query': 'SELECT COUNT(*) FROM songs WHERE title IS NULL', 'pass_result': 0},
        {'sql_query': 'SELECT COUNT(*) FROM artists WHERE name IS NULL', 'pass_result': 0},
        {'sql_query': 'SELECT COUNT(*) FROM users WHERE level IS NULL', 'pass_result': 0}
    ]
)




end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator