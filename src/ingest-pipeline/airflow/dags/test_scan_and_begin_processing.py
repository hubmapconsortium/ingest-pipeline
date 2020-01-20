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

with DAG('test_scan_and_begin_processing', 
         schedule_interval=None, 
         is_paused_upon_creation=False, 
         default_args=default_args) as dag:


    def set_params(*argv, **kwargs):
        # In the real version these will be set from kwargs['dag_run'].conf dict
        run_id = kwargs['run_id']
        ingest_id = run_id
        dag_params = Variable.get("dag_params", deserialize_json=True)
        new_dag_params = kwargs['params'].copy()
        new_dag_params['run_id'] = run_id
        new_dag_params['ingest_id'] = ingest_id
        Variable.set("dag_params", new_dag_params, serialize_json=True)
        return 'Whatever you return gets printed in the logs'

 
    def test_params(*argv, **kwargs):
        run_id = kwargs['run_id']
        dag_params = Variable.get("dag_params", deserialize_json=True)
        print('dag_params:')
        pprint(dag_params)
        print('kwargs:')
        pprint(kwargs)
        return 'maybe this will make it run only once'

         
    def send_status_msg(**kwargs):
        md_extract_retcode = int(kwargs['ti'].xcom_pull(task_ids="run_md_extract"))
        print('md_extract_retcode: ', md_extract_retcode)
        http_conn_id='ingest_api_connection'
        endpoint='/datasets/status'
        method='POST'
        headers={
            #'authorization' : 'Bearer ' + kwargs['params']['auth_tok'],
                 'content-type' : 'application/json'}
        extra_options=[]
         
        http = HttpHook(method,
                        http_conn_id=http_conn_id)
 
        md_fname = os.path.join(os.environ['AIRFLOW_HOME'],
                                    'data/temp', kwargs['run_id'],
                                    'rslt.yml')
        if md_extract_retcode == 0:
            md_fname = os.path.join(os.environ['AIRFLOW_HOME'],
                                    'data/temp', kwargs['run_id'],
                                    'rslt.yml')
            with open(md_fname, 'r') as f:
                md = yaml.safe_load(f)
            data = {'ingest_id' : kwargs['run_id'],
                    #'ingest_id' : kwargs['dag_run'].conf['ingest_id'],
                    'status' : 'success',
                    'message' : 'the process ran',
                    'metadata': md}
        else:
            log_fname = os.path.join(os.environ['AIRFLOW_HOME'],
                                     'data/temp', kwargs['run_id'],
                                     'session.log')
            with open(log_fname, 'r') as f:
                err_txt = '\n'.join(f.readlines())
            data = {'ingest_id' : kwargs['run_id'],
                    #'ingest_id' : kwargs['dag_run'].conf['ingest_id'],
                    'status' : 'failure',
                    'message' : err_txt}
        print('data: ', data)
         
 
    t0 = PythonOperator(
        task_id='set_params',
        python_callable=set_params,
        provide_context=True
        )

 
    t1 = PythonOperator(
        task_id='test_params',
        python_callable=test_params,
        provide_context=True
        )


    t_create_tmpdir = BashOperator(
        task_id='create_temp_dir',
        bash_command='mkdir ${AIRFLOW_HOME}/data/temp/{{run_id}}',
        provide_context=True
        )

 
        #lz_dir="{{dag_run.conf.lz_path}}" ; \
        #src_dir="{{dag_run.conf.src_path}}/md" ; \         
    t_run_md_extract = BashOperator(
        task_id='run_md_extract',
        bash_command=""" \
        lz_dir="{{var.json.dag_params.lz_path}}" ; \
        src_dir="{{var.json.dag_params.src_path}}/md" ; \
        work_dir="${AIRFLOW_HOME}/data/temp/{{run_id}}" ; \
        cd $work_dir ; \
        python $src_dir/metadata_extract.py --out ./rslt.yml --yaml "$lz_dir" \
          > ./session.log 2>&1 ; \
        echo $?
        """,
        provide_context = True
        )

 
    t_send_status = PythonOperator(
        task_id='send_status_msg',
        python_callable=send_status_msg,
        provide_context=True
    )
 
 
    t_cleanup_tmpdir = BashOperator(
        task_id='cleanup_temp_dir',
        bash_command='echo rm -r ${AIRFLOW_HOME}/data/temp/{{run_id}}',
        )
 
 
    dag >> t0 >> t1 >> t_create_tmpdir >> t_run_md_extract >> t_send_status >> t_cleanup_tmpdir
