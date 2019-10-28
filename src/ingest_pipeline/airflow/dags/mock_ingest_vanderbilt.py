from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

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
}

with DAG('mock_ingest_vanderbilt', schedule_interval=None, default_args=default_args) as dag:


    t1 = BashOperator(
        task_id='create_temp_dir',
        bash_command='mkdir ${AIRFLOW_HOME}/data/temp/{{run_id}}',
        )
    
    t2 = BashOperator(
        task_id='gen_output_metadata',
        bash_command='cp ${AIRFLOW_HOME}/data/mock_data/{{dag_run.conf["process"]}}.yaml ${AIRFLOW_HOME}/data/temp/{{run_id}}',
        )

    dag >> t1 >> t2

