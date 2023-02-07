#!/usr/bin/env python3

# Import modules
import http
import datetime

from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.configuration import conf as airflow_conf

import utils
from utils import (
    HMDAG,
    get_tmp_dir_path,
    get_auth_tok,
    find_matching_endpoint,
    get_queue_resource,
    get_preserve_scratch_resource,
    )

# Simple call to one files-api endpoint in AWS, cut down from the sample code recommended by Joel at
# https://github.com/hubmapconsortium/ingest-pipeline/blob/devel/src/ingest-pipeline/airflow/dags/generate_usage_report.py

default_args = {
    'owner': 'hubmap',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 2, 1),
    'email': ['joel.welling@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'xcom_push': True,
    'queue': get_queue_resource('trigger_file_reindex')
}

with HMDAG('trigger_file_reindex',
           schedule_interval='@weekly',
           is_paused_upon_creation=False,
           default_args=default_args,
           user_defined_macros={
               'tmp_dir_path': get_tmp_dir_path,
               'preserve_scratch': get_preserve_scratch_resource('trigger_file_reindex'),
           }) as dag:

    def launch_file_reindex(**kwargs):
        auth_token = get_auth_tok(**kwargs)

        try:
            headers = {
                'authorization': 'Bearer ' + auth_token,
                'content-type': 'text/plain',
                'X-Hubmap-Application': 'files-api'
            }
            response = HttpHook('PUT', http_conn_id='files_api_connection').run(
                endpoint=f'datasets/refresh-indices',
                headers=headers,
                extra_options=[]
            )
            response.raise_for_status()
        except http.client.HTTPException as he:
            print(f'Error {he}')

    t_launch_file_reindex = PythonOperator(
        task_id='launch_file_reindex',
        python_callable=launch_file_reindex,
        provide_context=True,
        op_kwargs={
            'crypt_auth_tok': utils.encrypt_tok(airflow_conf.as_dict()
                                                ['connections']['APP_CLIENT_SECRET']).decode(),
            }
        )

    t_launch_file_reindex