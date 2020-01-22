from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.hooks.http_hook import HttpHook
from airflow.configuration import conf
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
    'provide_context': True
}

with DAG('mock_ingest_rnaseq_10x', schedule_interval=None, is_paused_upon_creation=False,
         default_args=default_args) as dag:

    def print_context(*argv, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        print('kwargs: ')
        pprint(kwargs)
        print('argv: ')
        pprint(argv)
        print('dag_run conf:')
        pprint(kwargs['dag_run'].conf)
        print('raw conf:')
        pprint(kwargs['conf'].as_dict())
        return 'Whatever you return gets printed in the logs'


    def extract_and_save_md(*argv, **kwargs):
        in_fname = os.path.join(os.environ['AIRFLOW_HOME'],
                                'dags/mock_data',
                                kwargs['dag_run'].conf['process'] + '.yml')
        out_fname = os.path.join(os.environ['AIRFLOW_HOME'],
                                 'data/temp', kwargs['run_id'],
                                 'rslt.yml')
        with open(in_fname, 'r') as f:
            raw_md = yaml.safe_load(f)
        with open(out_fname, 'w') as f:
            yaml.safe_dump(raw_md['metadata'], f)


    def send_status_msg(**kwargs):
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
        with open(md_fname, 'r') as f:
            md = yaml.safe_load(f)
        data = {'ingest_id' : kwargs['dag_run'].conf['ingest_id'],
                'status' : 'success',
                'message' : 'the process ran',
                'metadata': md}
#         data = {'ingest_id' : kwargs['params']['ingest_id'],
#                 'status' : 'failure',
#                 'message' : 'this is a sample error message'
#                 }
        print('data: ', data)
        print("Calling HTTP method")

        response = http.run(endpoint,
                            json.dumps(data),
                            headers,
                            extra_options)
        print(response.text)

    t0 = PythonOperator(
        task_id='print_the_context',
        python_callable=print_context
        )

    t1 = BashOperator(
        task_id='create_temp_dir',
        bash_command='mkdir ${AIRFLOW_HOME}/data/temp/{{run_id}}',
        )
    
    t2 = PythonOperator(
        task_id='gen_output_metadata',
        python_callable=extract_and_save_md
        )

    t3 = PythonOperator(
        task_id='send_status_msg',
        python_callable=send_status_msg
        )

    t_cleanup_tmpdir = BashOperator(
        task_id='cleanup_temp_dir',
        bash_command='echo rm -r ${AIRFLOW_HOME}/data/temp/{{run_id}}',
        )
 
    dag >> t0 >> t1 >> t2 >> t3 >> t_cleanup_tmpdir

