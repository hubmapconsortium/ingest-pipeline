#!/usr/bin/env python3

# Import modules
import http
import datetime

from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.configuration import conf as airflow_conf
from airflow.exceptions import AirflowException

import utils
from utils import (
    HMDAG,
    get_tmp_dir_path,
    get_auth_tok,
    find_matching_endpoint,
    get_queue_resource,
    get_preserve_scratch_resource,
    )

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
    'queue': get_queue_resource('trigger_samples_incremental_reindex'),
    "executor_config": {"SlurmExecutor": {"slurm_output_path": "/hive/users/hive/airflow-logs/slurm/"}},
}

with HMDAG('trigger_samples_incremental_reindex',
           schedule_interval='@weekly',
           is_paused_upon_creation=False,
           default_args=default_args,
           user_defined_macros={
               'tmp_dir_path': get_tmp_dir_path,
               'preserve_scratch': get_preserve_scratch_resource('trigger_samples_incremental_reindex'),
           }) as dag:

    def launch_samples_incremental_reindex(**kwargs):
        auth_token = get_auth_tok(**kwargs)

        try:
            headers = {
                'authorization': 'Bearer ' + auth_token,
                'content-type': 'text/plain',
                'X-Hubmap-Application': 'files-api'
            }
            # URL Root: https://spatial.api.hubmapconsortium.org/
            response = HttpHook('PUT', http_conn_id='spatial_api_connection').run(
                endpoint='samples/incremental-reindex',
                headers=headers,
                extra_options=[]
            )
            response.raise_for_status()
        except http.client.HTTPException as he:
            print(f'Error {he}')
        except AirflowException as ae:
            print(f'Error {ae}')
        except Exception as e:
            print(f'Broad Exception {e}')

    t_launch_samples_incremental_reindex = PythonOperator(
        task_id='launch_samples_incremental_reindex',
        python_callable=launch_samples_incremental_reindex,
        provide_context=True,
        op_kwargs={
            'crypt_auth_tok': utils.encrypt_tok(airflow_conf.as_dict()
                                                ['connections']['APP_CLIENT_SECRET']).decode(),
            }
        )

    t_launch_samples_incremental_reindex
