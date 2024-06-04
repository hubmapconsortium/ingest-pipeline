from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import globus_sdk
from utils import HMDAG, get_queue_resource

default_args = {
    'owner': 'hubmap',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email': ['jpuerto@psc.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'queue': get_queue_resource('globus_transfer'),
    "executor_config": {"SlurmExecutor": {"slurm_output_path": "/hive/users/hive/airflow-logs/slurm/%x_%N_%j.out"}},
}

with HMDAG('globus_transfer',
           schedule_interval=None,
           is_paused_upon_creation=False,
           default_args=default_args
           ) as dag:
    def perform_transfer(*argv, **kwargs):
        dag_run_conf = kwargs['dag_run'].conf
        globus_transfer_token = dag_run_conf['tokens']['transfer.api.globus.org']['access_token']
        src_epid = 'ff1bd56e-2e65-4ec9-86fa-f79422884e96'
        dest_epid = 'bcc55f9a-63d3-4c05-8da4-7eaa4d215f33'
        authorizer = globus_sdk.AccessTokenAuthorizer(globus_transfer_token)
        tc = globus_sdk.TransferClient(authorizer=authorizer)
        tc.endpoint_autoactivate(src_epid)
        tc.endpoint_autoactivate(dest_epid)

        td = globus_sdk.TransferData(tc, src_epid, dest_epid, label='Transfer')

        for transfer_item in dag_run_conf['conf']['transfer_items']:
            try:
                td.add_item(transfer_item['src'], transfer_item['dest'],
                            recursive=transfer_item.get('recursive', False))
            except Exception as e:
                print(e)
                continue

        tc.submit_transfer(td)

    t0 = PythonOperator(
        task_id='perform_transfer',
        python_callable=perform_transfer,
        provide_context=True
    )

    t0
