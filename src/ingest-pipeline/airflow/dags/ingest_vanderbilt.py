from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.hooks.http_hook import HttpHook
from airflow.configuration import conf
from airflow.models import Variable
from datetime import datetime, timedelta
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
}

with DAG('ingest_vanderbilt', 
         schedule_interval=None, 
         is_paused_upon_creation=False, 
         default_args=default_args) as dag:

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
            data = {'ingest_id' : kwargs['dag_run'].conf['ingest_id'],
                    'status' : 'success',
                    'message' : 'the process ran',
                    'metadata': md}
        else:
            log_fname = os.path.join(os.environ['AIRFLOW_HOME'],
                                     'data/temp', kwargs['run_id'],
                                     'session.log')
            with open(log_fname, 'r') as f:
                err_txt = '\n'.join(f.readlines())
            data = {'ingest_id' : kwargs['dag_run'].conf['ingest_id'],
                    'status' : 'failure',
                    'message' : err_txt}
        print('data: ', data)

        response = http.run(endpoint,
                            json.dumps(data),
                            headers,
                            extra_options)
        print('response: ', response.text)
        

    t_create_tmpdir = BashOperator(
        task_id='create_temp_dir',
        bash_command='mkdir ${AIRFLOW_HOME}/data/temp/{{run_id}}',
        provide_context=True
        )


    t_run_md_extract = BashOperator(
        task_id='run_md_extract',
        bash_command=""" \
        lz_dir="/hive/hubmap/lz/{{dag_run.conf.provider}}/{{dag_run.conf.submission_id}}" ; \
        work_dir="${AIRFLOW_HOME}/data/temp/{{run_id}}" ; \
        src_dir="/hive/users/welling/git/hubmap/ingest-pipeline/src/ingest-pipeline/md" ; \
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
        bash_command='rm -r ${AIRFLOW_HOME}/data/temp/{{run_id}}',
        )

    dag >> t_create_tmpdir >> t_run_md_extract >> t_send_status >> t_cleanup_tmpdir

