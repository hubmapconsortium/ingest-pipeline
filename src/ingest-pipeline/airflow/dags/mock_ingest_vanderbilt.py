from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
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

with DAG('mock_ingest_vanderbilt', schedule_interval=None, is_paused_upon_creation=False, default_args=default_args) as dag:

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
                                'data/mock_data',
                                kwargs['params']['process'] + '.yml')
        out_fname = os.path.join(os.environ['AIRFLOW_HOME'],
                                 'data/temp', kwargs['run_id'],
                                 'rslt.yml')
        with open(in_fname, 'r') as f:
            raw_md = yaml.safe_load(f)
        with open(out_fname, 'w') as f:
            yaml.safe_dump(raw_md['metadata'], f)


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
    
    t3 = SimpleHttpOperator(
        task_id='pass_md_to_REST',
        http_conn_id='ingest_api_url',
        endpoint='/datasets/status',
        method='POST',
        data=json.dumps({}),
        log_response=True
        )

    dag >> t0 >> t1 >> t2 >> t3

