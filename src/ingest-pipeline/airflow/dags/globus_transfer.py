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
import globus_sdk
from airflow.utils.session import provide_session

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
    @provide_session
    def perform_transfer(session=None, **kwargs):
        response_data = []
        token_response = session['tokens']
        globus_transfer_token = token_response['transfer.api.globus.org']['access_token']
        base_path_to_transfer = 'juan-testing'
        src_dir_to_transfer = 'juan-testing'
        dest_dir_to_transfer = 'juan-testing'
        hive_epid = 'ff1bd56e-2e65-4ec9-86fa-f79422884e96'
        aws_epid = '1c652c81-3833-4af2-a0b1-01c70805a87d'
        authorizer = globus_sdk.AccessTokenAuthorizer(globus_transfer_token)
        tc = globus_sdk.TransferClient(authorizer=authorizer)
        tc.endpoint_autoactivate(hive_epid)
        tc.endpoint_autoactivate(aws_epid)

        td = globus_sdk.TransferData(tc, hive_epid, aws_epid, label='test transfer')

        # Iterate over the files
        for entry in tc.operation_ls(hive_epid, path=base_path_to_transfer):
            response_data.append(entry)

        for entry in tc.operation_ls(aws_epid, path=base_path_to_transfer):
            response_data.append(entry)

        td.add_item(src_dir_to_transfer, dest_dir_to_transfer, recursive=True)

        tr = tc.submit_transfer(td)

        print(tr)


    t0 = PythonOperator(
        task_id='print_the_context',
        python_callable=perform_transfer
    )



    dag >> t0
