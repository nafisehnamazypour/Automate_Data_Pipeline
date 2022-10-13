from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')


S3_BUCKET = 'udacity-dend' 
S3_SONG_KEY = 's3://udacity-dend/song_data'
S3_LOG_KEY = 'log_data/{execution_date.year}/{execution_date.month}'
LOG_JSON_PATH = 's3://udacity-dend/log_json_path.json'
REGION = 'us-west-2'


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False          
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,    
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    table = 'staging_events',
    s3_bucket = S3_BUCKET,
    s3_key = S3_LOG_KEY,
    region = REGION,
    truncate = False,
    data_format = f"JSON '{LOG_JSON_PATH}'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_songs',
    dag = dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    table = 'staging_songs',
    s3_bucket = S3_BUCKET,
    s3_key = S3_SONG_KEY,
    region = REGION,
    truncate = True,
    data_format = "JSON 'auto'"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag = dag,
    postgres_conn_id = 'redshift',
    sql = SqlQueries.songplay_table_insert,
    table = 'songplays',
    truncate = False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    postgres_conn_id = 'redshift',
    sql = SqlQueries.user_table_insert,
    table = 'users',
    truncate = True    
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    postgres_conn_id = 'redshift',
    sql = SqlQueries.song_table_insert,
    table = 'songs',
    truncate = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    postgres_conn_id = 'redshift',
    sql = SqlQueries.artist_table_insert,
    table = 'artists',
    truncate = True 
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    postgres_conn_id = 'redshift',
    sql = SqlQueries.time_table_insert,
    table = 'time',
    truncate = True 
)


tables_list = ['staging_events', 'staging_songs', 'songplays', 'users', 'songs', 'artists', 'time']
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id='redshift',
    test_query='select count(*) from songs where songid is null;',
    expected_result=0,
    dag=dag
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table


load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator