#!/usr/bin/env python3

# Import modules
import os
import shutil
import re
import hubmap_sdk
import pandas
import subprocess
import datetime
from hubmap_sdk import EntitySdk

from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.configuration import conf as airflow_conf

from hubmap_operators.common_operators import CreateTmpDirOperator, CleanupTmpDirOperator

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
    'email': ['@TODO-KBKBKB-is this someone at PSC? Can I know about starts, stops, and failures?'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1, #KBKBKB-what causes a retry? Anticipate HTTP 200 response within 30 seconds, but maybe better not to retry until next week? Do not want two of these running at once.
    'retry_delay': datetime.timedelta(minutes=1), #KBKBKB-do we need this?  Set to 0 to disable? Else 5*24*3600-ish?
    'xcom_push': True,
    'queue': get_queue_resource('trigger_file_reindex')
}

with HMDAG('trigger_file_reindex',
           schedule_interval='@weekly', #KBKBKB-maybe daily is "better"? What are considerations for PSC?
           is_paused_upon_creation=False,
           default_args=default_args,
           user_defined_macros={
               'tmp_dir_path': get_tmp_dir_path, # I don't need a tmp dir for my work, like example.  Does AirFlow need one for job execution?
               'preserve_scratch': get_preserve_scratch_resource('trigger_file_reindex'),
           }) as dag:

    def launch_file_reindex(**kwargs):
        auth_token = get_auth_tok(**kwargs)
        files_api_connection = HttpHook.get_connection('files_api_connection').host # KBKBKB-where is files_api_connection configured?
        instance_identifier = find_matching_endpoint(files_api_connection)
        # This is intended to throw an error if the instance is unknown or not listed #KBKBKB-Do I need the verification side effect, even if I don't need output_dir?
        output_dir = {'PROD':  '/hive/hubmap/assets/status',
                      'STAGE': '/hive/hubmap-stage/assets/status',
                      'TEST':  '/hive/hubmap-test/assets/status',
                      'DEV':   '/hive/hubmap-dev/assets/status'}[instance_identifier]
        # KBKBKB-Now that I'm here, I'm not quite sure
        # KBKBKB-I need instance_identifier, esp. if I
        # KBKBKB-don't need output_dir. When HttpHook below
        # KBKBKB-uses files_api_connection, is that an old
        # KBKBKB-style replaced by find_matching_endpoint(), or is
        # KBKBKB-find_matching_endpoint() connected with SDK usage??????????

        try: #KBKBKB-@TODO-Confirm the code block below can raise an HTTPException of some sort, besides hubmap_sdk.sdk_helper.HTTPException
            # KBKBKB-Since files-api won't have an SDK, try grabbing a straight endpoint call from
            # KBKBKB-https://github.com/hubmapconsortium/ingest-pipeline/blob/39532b92b28a6e3ff8e028be966ef7bed509281d/src/ingest-pipeline/airflow/dags/launch_checksums.py#L134
            # KBKBKB-Change send_block call to uuid-api /hmuuid POST to files-api /datasets/refresh-indices PUT
            headers = {
                'authorization': 'Bearer ' + auth_token,
                'content-type': 'text/plain',
                'X-Hubmap-Application': 'files-api'
            }
            response = HttpHook('POST', http_conn_id='files_api_connection').run(
                endpoint=f'datasets/refresh-indices',
                headers=headers,
                extra_options=[]
            )
            response.raise_for_status()
            print('response block follows')
            pprint(response.json())
        except http.client.HTTPException as he:
            print(f'Error {he}') #KBKBKB-does this cause logging, email notification, etc?

        #KBKBKB-how about a "success" notification here?  Is that handled by the framework that called this?

# KBKBKB-what exactly do I need from here down????????????????
    t_launch_file_reindex = PythonOperator(
        task_id='launch_file_reindex',
        python_callable=launch_file_reindex,
        provide_context=True,
        op_kwargs={
            'crypt_auth_tok': utils.encrypt_tok(airflow_conf.as_dict()
                                                ['connections']['APP_CLIENT_SECRET']).decode(),
            }
        )
    
    t_create_tmpdir = CreateTmpDirOperator(task_id='create_tmpdir')
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id='cleanup_tmpdir')

    (
        t_create_tmpdir
        >> t_launch_file_reindex
        >> t_cleanup_tmpdir
    )

t_send_checksums = PythonOperator(
    task_id='send_checksums',
    python_callable=send_checksums,
    provide_context=True,
    op_kwargs={
        'crypt_auth_tok': utils.encrypt_tok(airflow_conf.as_dict()
                                            ['connections']['APP_CLIENT_SECRET']).decode(),
    }
)

t_create_tmpdir = CreateTmpDirOperator(task_id='create_tmpdir')
t_cleanup_tmpdir = CleanupTmpDirOperator(task_id='cleanup_tmpdir')

(
        t_create_tmpdir
        >> check_uuid_t
        >> t_gen_cksum_table
        >> t_send_checksums
        >> t_cleanup_tmpdir
)