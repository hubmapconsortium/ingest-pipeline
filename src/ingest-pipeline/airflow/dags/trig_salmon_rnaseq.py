from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.hooks.http_hook import HttpHook
from airflow.configuration import conf
from airflow.models import Variable
from datetime import datetime, timedelta
import pytz
from pprint import pprint
import os
import yaml
import json

# Following are defaults which can be overridden later on
default_args = {
    'owner': 'hubmap',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email': ['joel.welling@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'provide_context': True,
    'xcom_push': True,
    'params': {
            'auth_tok': 'AgJEbW7KYQVDvpdWJ9vJWGDrYa0W0zp5vJ3e1vP3mz8ydzOjO6T0CBxzxEnKmJeOedyx58j44EDDM7U395N0Yf7KVD',
            'dag_id': 'test_scan_and_begin_processing_dag_id',
            'process': 'scan.and.begin.processing',
            'provider': 'IEC Testing Group',
            'execution_date': datetime.now(tz = pytz.timezone('America/New_York')).isoformat(),
            'lz_path': "/usr/local/airflow/lz/IEC Testing Group/80cd0a1fadbb654f023daf197d8a0bfe",
            'src_path': "/usr/src/app/src",
            #'submission_id': '43c6ad5c6b2471ca812ac8fa4029d63a', # original
            #'submission_id': '404cf7a1c0149e3cccbd7b50b09d28d3', # good Vanderbilt copy
            #'submission_id': '4f80da2501a9547fbbb6f2a1b9458633', # bad Vanderbilt copy
            'submission_id': '80cd0a1fadbb654f023daf197d8a0bfe',  # good Florida copy
        }
}

with DAG('trig_salmon_rnaseq', 
         schedule_interval=None, 
         is_paused_upon_creation=False, 
         default_args=default_args) as dag:

 
    def trigger_target(**kwargs):
        ctx = kwargs['dag_run'].conf
        run_id = kwargs['run_id']
        print('run_id: ', run_id)
        print('dag_run.conf:')
        pprint(ctx)
        print('kwargs:')
        pprint(kwargs)



    t1 = PythonOperator(
        task_id='trigger_target',
        python_callable = trigger_target,
        )


#     t_create_tmpdir = BashOperator(
#         task_id='create_temp_dir',
#         bash_command='mkdir ${AIRFLOW_HOME}/data/temp/{{run_id}}',
#         provide_context=True
#         )
# 
#  
#     t_send_status = PythonOperator(
#         task_id='send_status_msg',
#         python_callable=send_status_msg,
#         provide_context=True
#     )
 
  
    dag >> t1 # >> t_create_tmpdir >> t_cleanup_tmpdir
