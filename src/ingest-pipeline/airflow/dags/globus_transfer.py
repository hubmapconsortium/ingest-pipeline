from airflow import DAG, models
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import globus_sdk

default_args = {
    'owner': 'hubmap',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email': ['jpuerto@psc.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG('globus_transfer', schedule_interval=None, is_paused_upon_creation=False, default_args=default_args) as dag:
    def perform_transfer(*argv, **kwargs):
        dag_run_conf = kwargs['dag_run'].conf
        globus_transfer_token = dag_run_conf['tokens']['transfer.api.globus.org']['access_token']
        src_dir_to_transfer = dag_run_conf['src_dir_to_transfer']
        dest_dir_to_transfer = dag_run_conf['dest_dir_to_transfer']
        hive_epid = 'ff1bd56e-2e65-4ec9-86fa-f79422884e96'
        aws_epid = '1c652c81-3833-4af2-a0b1-01c70805a87d'
        authorizer = globus_sdk.AccessTokenAuthorizer(globus_transfer_token)
        tc = globus_sdk.TransferClient(authorizer=authorizer)
        tc.endpoint_autoactivate(hive_epid)
        tc.endpoint_autoactivate(aws_epid)

        td = globus_sdk.TransferData(tc, hive_epid, aws_epid, label='Transfer')
        td.add_item(src_dir_to_transfer, dest_dir_to_transfer, recursive=True)
        tc.submit_transfer(td)

    t0 = PythonOperator(
        task_id='perform_transfer',
        python_callable=perform_transfer,
        provide_context=True
    )



    dag >> t0