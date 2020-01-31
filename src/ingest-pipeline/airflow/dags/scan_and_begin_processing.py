from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
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


with DAG('scan_and_begin_processing', 
         schedule_interval=None, 
         is_paused_upon_creation=False, 
         default_args=default_args) as dag:
        
    def send_status_msg(**kwargs):
        md_extract_retcode = int(kwargs['ti'].xcom_pull(task_ids="run_md_extract"))
        print('md_extract_retcode: ', md_extract_retcode)
        http_conn_id='ingest_api_connection'
        endpoint='/datasets/status'
        method='PUT'
        headers={'authorization' : 'Bearer ' + kwargs['dag_run'].conf['auth_tok'],
                 'content-type' : 'application/json'}
        extra_options=[]
         
        http = HttpHook(method,
                        http_conn_id=http_conn_id)
 
        if md_extract_retcode == 0:
            md_fname = os.path.join(os.environ['AIRFLOW_HOME'],
                                    'data/temp', kwargs['run_id'],
                                    'rslt.yml')
            with open(md_fname, 'r') as f:
                md = yaml.safe_load(f)
            data = {'dataset_id' : kwargs['dag_run'].conf['submission_id'],
                    'status' : 'QA',
                    'message' : 'the process ran',
                    'metadata': md}
            kwargs['ti'].xcom_push(key='collectiontype',
                                   value=(md['collectiontype'] if 'collectiontype' in md
                                          else None))
        else:
            log_fname = os.path.join(os.environ['AIRFLOW_HOME'],
                                     'data/temp', kwargs['run_id'],
                                     'session.log')
            with open(log_fname, 'r') as f:
                err_txt = '\n'.join(f.readlines())
            data = {'dataset_id' : kwargs['dag_run'].conf['submission_id'],
                    'status' : 'Invalid',
                    'message' : err_txt}
            kwargs['ti'].xcom_push(key='collectiontype', value=None)
        print('data: ', data)
         
        response = http.run(endpoint,
                            json.dumps(data),
                            headers,
                            extra_options)
        print('response: ', response.text)

    def maybe_spawn_dag(context, dag_run_obj):
        """
        This needs to be table-driven.  Its purpose is to suppress
        triggering the dependent DAG if the collectiontype doesn't
        have a dependent DAG.
        """
        print('context:')
        pprint(context)
        md_extract_retcode = int(context['ti'].xcom_pull(task_ids="run_md_extract"))
        collectiontype = context['ti'].xcom_pull(key='collectiontype',
                                                 task_ids='send_status_msg')
        if md_extract_retcode == 0 and collectiontype is not None:
            md_fname = os.path.join(os.environ['AIRFLOW_HOME'],
                                    'data/temp', context['run_id'],
                                    'rslt.yml')
            with open(md_fname, 'r') as f:
                md = yaml.safe_load(f)
            if collectiontype in ['rnaseq_10x']:
                payload = {'ingest_id' : context['run_id'],
                           #'ingest_id' : context['dag_run'].conf['ingest_id'],
                           'auth_tok' : context['dag_run'].conf['auth_tok'],
                           'parent_lz_path' : context['dag_run'].conf['lz_path'],
                           'parent_submission_id' : context['dag_run'].conf['submission_id'],
                           'metadata': md}
                dag_run_obj.payload = payload
                return dag_run_obj
            else:
                print('Extracted metadata has no "collectiontype" element')
                return None
        else:
            # The extraction of metadata failed or collectiontype is unknown,
            # so spawn no child runs
            return None


    def trigger_or_skip(**kwargs):
        collectiontype = kwargs['ti'].xcom_pull(key='collectiontype',
                                                task_ids="send_status_msg")
        if collectiontype is None:
            return 'no_spawn'
        else:
            return 'maybe_spawn_dag'

    t_create_tmpdir = BashOperator(
        task_id='create_temp_dir',
        bash_command='mkdir ${AIRFLOW_HOME}/data/temp/{{run_id}}',
        provide_context=True
        )


    t_run_md_extract = BashOperator(
        task_id='run_md_extract',
        bash_command=""" \
        lz_dir="{{dag_run.conf.lz_path}}" ; \
        src_dir="{{dag_run.conf.src_path}}/md" ; \
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

    t_maybe_trigger = BranchPythonOperator(
        task_id='maybe_trigger',
        python_callable=trigger_or_skip,
        provide_context=True
        )

    t_join = DummyOperator(
        task_id='join',
        trigger_rule='one_success')

    t_no_spawn = DummyOperator(
        task_id='no_spawn')

    t_spawn_dag = TriggerDagRunOperator(
        task_id="maybe_spawn_dag",
        trigger_dag_id="trig_{{ti.xcom_pull(key='collectiontype', task_ids='send_status_msg')}}",
        python_callable = maybe_spawn_dag,
        #conf={"message": "Hello World"},
        #provide_context = True
        )
 

    t_cleanup_tmpdir = BashOperator(
        task_id='cleanup_temp_dir',
        bash_command='rm -r ${AIRFLOW_HOME}/data/temp/{{run_id}}',
        )

    (dag >> t_create_tmpdir >> t_run_md_extract >> t_send_status
     >> t_maybe_trigger)
    t_maybe_trigger >> t_spawn_dag >> t_join
    t_maybe_trigger >> t_no_spawn >> t_join
    t_join >> t_cleanup_tmpdir

