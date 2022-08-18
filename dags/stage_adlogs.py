import os
import logging

from airflow.decorators import dag, task
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from datetime import datetime, timedelta
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.operators.python import PythonOperator 

# custom utils
from utils.job_config import JobConfig

JOB_ARGS = JobConfig.get_config()
DEFAULTS = JOB_ARGS["default_args"]
ENV = JOB_ARGS["env_name"]
TEAM_NAME = JOB_ARGS["team_name"]
SF_CONN_ID = JOB_ARGS["snowflake_conn_id"]
SF_ROLE = JOB_ARGS["snowflake"]["role"]
SF_WAREHOUSE = JOB_ARGS["snowflake"]["warehouse"]
SF_DATABASE = JOB_ARGS["snowflake"]["database"]
aws_bucket_name = JOB_ARGS['aws_bucket_name']
aws_conn_id = JOB_ARGS['aws_conn_id']
filters = JOB_ARGS['header_check']['app_live']['filters']
folder  = JOB_ARGS['folder']
data_source = JOB_ARGS['data_source']

stage_sql_path = JOB_ARGS['stage_sql_path']
transform_sql_path = JOB_ARGS['transform_sql_path']

from utils.py_header_check import pyHeaderCheck
# create DAG
dag_id = 'stage_adlogs'

@dag(
dag_id=dag_id,
start_date=datetime(2018, 1, 1),
max_active_runs=1,
schedule_interval=JOB_ARGS['schedule_interval'],
default_args=DEFAULTS,
default_view="tree", # This defines the default view for this DAG in the Airflow UI
catchup=False,
tags=["Airflow2.0"], # If set, this tag is shown in the DAG view of the Airflow UI
)
def stage_simple_dag():

    stage_finish = DummyOperator(task_id="adlogs_snowflake_staging_finish")
    count = 0

    # staging ad logs hourly
    for table in JOB_ARGS["tables"]:

        table_column_names = JOB_ARGS[f'{table}_columns']


        print(table_column_names)


        stage_adlogs_check = S3KeySensor(
            task_id = 'waiting_for_data_{}'.format(table),
            bucket_key = 'raw-ingester-out/manifests/{}/20190704/15/completed.manifest'.format(table),
            bucket_name = 'das42-airflow-training',
            aws_conn_id = 'aws_airflow')


        stage_adlogs_header_check = PythonOperator(
            task_id = 'header_check_{}'.format(table),
            python_callable = pyHeaderCheck,
            op_kwargs = {
                'aws_bucket_name' : aws_bucket_name,
                'filepath' : '{}/{}/*'.format(folder[count],data_source),
                's3_conn' : aws_conn_id,
                'table_config' : table,
                'columns' : table_column_names,
                'filters' : filters,
                'source' : data_source
                }
            )


        stage_adlogs_hourly_job = SnowflakeOperator(
            task_id="stage_adlogs_{}_hourly".format(table),
            snowflake_conn_id=SF_CONN_ID,
            warehouse=SF_WAREHOUSE,
            database=SF_DATABASE,
            sql=f'sql/{stage_sql_path}/{table}.sql',
            params={
                "env": ENV,
                "team_name": TEAM_NAME
            },
            autocommit=True,
            trigger_rule='all_done'
        )

        stage_adlogs_transform = SnowflakeOperator(
            task_id = "stage_transform_{}".format(table),
            snowflake_conn_id=SF_CONN_ID,
            warehouse=SF_WAREHOUSE,
            database=SF_DATABASE,
            sql=f'sql/{transform_sql_path}/transform_{table}.sql',
            params={
                "env": ENV,
                "team_name": TEAM_NAME
            },
            autocommit=True,
            trigger_rule='all_done'
        )

        count += 1

        stage_adlogs_check >> stage_adlogs_header_check >> stage_adlogs_hourly_job >> stage_adlogs_transform >> stage_finish


stage_simple_dag = stage_simple_dag()
