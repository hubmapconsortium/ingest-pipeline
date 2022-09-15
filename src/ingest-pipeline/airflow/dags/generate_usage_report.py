#!/usr/bin/env python3

# Import modules
import os
import shutil
import re
import tempfile
import hubmap_sdk
import pandas
import sys
import subprocess
import datetime
from hubmap_sdk import EntitySdk

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.exceptions import AirflowException
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

default_args = {
    'owner': 'hubmap',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 5, 1),
    'email': ['joel.welling@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'xcom_push': True,
    'queue': get_queue_resource('generate_usage_report')
}

with HMDAG('generate_usage_report',
           schedule_interval='@weekly',
           is_paused_upon_creation=False,
           default_args=default_args,
           user_defined_macros={
               'tmp_dir_path': get_tmp_dir_path,
               'preserve_scratch': get_preserve_scratch_resource('generate_usage_report'),
           },
       ) as dag:

    def build_report(**kwargs):
        entity_token = get_auth_tok(**kwargs)
        usage_csv = '/hive/hubmap/data/usage-reports/Globus_Usage_Transfer_Detail.csv'
        log_directory = '/hive/users/backups/app001/var/log/gridftp-audit/'
        entity_host = HttpHook.get_connection('entity_api_connection').host
        instance_identifier = find_matching_endpoint(entity_host)
        # This is intended to throw an error if the instance is unknown or not listed
        output_dir = {'PROD' :  '/hive/hubmap/assets/status',
                      'STAGE' : '/hive/hubmap-stage/assets/status',
                      'TEST' :  '/hive/hubmap-test/assets/status',
                      'DEV' :   '/hive/hubmap-dev/assets/status'}[instance_identifier]
        usage_output = f'{output_dir}/usage_report.json'

        temp_name = get_tmp_dir_path(kwargs['run_id'])

        # If this is a retry of this operation, there may be files in temp_name left
        # over from the previous pass.  Clear them.
        for filename in os.listdir(temp_name):
            os.remove(os.path.join(temp_name, filename))

        for filename in os.listdir(log_directory):
            f = os.path.join(log_directory, filename)
            destination = os.path.join(temp_name, filename)
            if filename.endswith(".gz"):
                shutil.copyfile(f, destination)
        try:
            subprocess.run(["gunzip", "-r", temp_name])
        except subprocess.CalledProcessError as e:
            print(f'gunzip of logs failed with {e}')
            raise

        # Settings pandas options, initializing dataframe "usage" from csv file,
        # and choosing endpoint/collection ids to track
        pandas.options.display.max_rows = 999999
        pandas.options.display.max_columns = 999999
        try:
            usage = pandas.read_csv(usage_csv)
        except (FileNotFoundError, ValueError) as e:
            print(f'Read usage csv failed: {e}')
            raise
        print(f'Read {len(usage)} recs from {usage_csv}')
        endpoint_ids = ["24c2ee95-146d-4513-a1b3-ac0bfdb7856f",
                        "af603d86-eab9-4eec-bb1d-9d26556741bb"]
        specific_usage_array = []

        # Filtering out entries from the CSV to only use ones from endpoint ids
        # important to us and within the date range of the log files we have.
        # Taking those rows and columns and creating a new data frame "specific_usage"
        for ind in usage.index:
            is_hubmap = False
            if usage["source_endpoint_id"][ind] in endpoint_ids:
                is_hubmap = True
            if usage["destination_endpoint_id"][ind] in endpoint_ids:
                is_hubmap = True
            if usage["source_endpoint_host_id"][ind] in endpoint_ids:
                is_hubmap = True
            if usage["destination_endpoint_host_id"][ind] in endpoint_ids:
                is_hubmap = True
            request_time = usage['request_time'][ind]
            request_datetime = datetime.datetime.strptime(request_time,
                                                          '%Y-%m-%d %H:%M:%S.%f')
            timestamp = datetime.datetime.timestamp(request_datetime)
            if is_hubmap and timestamp > 1630468800.0:
                new_series = {
                    "user_name": usage["user_name"][ind],
                    "request_time": usage["request_time"][ind],
                    "source_endpoint_id": usage["source_endpoint_id"][ind],
                    "source_endpoint_name": usage["source_endpoint"][ind],
                    "destination_endpoint_name": usage["destination_endpoint"][ind],
                    "destination_endpoint_id": usage["destination_endpoint_id"][ind],
                    "source_endpoint_host_id": usage["source_endpoint_host_id"][ind],
                    "destination_endpoint_host_id": usage["destination_endpoint_host_id"][ind],
                    "taskid": usage["taskid"][ind],
                    "bytes_transferred": usage["bytes_transferred"][ind]
                }
                specific_usage_array.append(new_series)
        specific_usage = pandas.DataFrame(specific_usage_array)
        print(f'specific_usage array has {len(specific_usage)} records')

        # Initializing empty lists. hubmap_id_column and data_types_column will be
        # converted into columns for specific_usage
        hubmap_id_column = []
        data_types_column = []
        entity_types_column = []
        rows_to_remove = []
        full_contents = []

        # read each of the files inside temp and keep them in memory:
        file_count = 0
        for filename in os.listdir(temp_name):
            try:
                f = os.path.join(temp_name, filename)
                with open(f, "r", encoding="utf8") as file:
                    lines = file.readlines()
                    for each in lines:
                        full_contents.append(each)
                file_count += 1
            except IOError as e:
                print(f'Error reading {filename}: {e}')
                raise
        print(f'full_contents list has {len(full_contents)} entries from {file_count} files')

        # For every record in the data frame, do:
        count = 0
        for ind in specific_usage.index:
            count = count + 1
            task_id = specific_usage['taskid'][ind]
            lines_with_tid = []
            dataset_id = None
            for line in full_contents:
                index = line.find(task_id)
                if index != -1:
                    lines_with_tid.append(line)

             # for every line that contained the task id we are looking for, we now
            # look for the reference number
            for line in lines_with_tid:
                log_entry = line.replace("-","")
                ref_index = log_entry.find("ref=") + 4
                reference_number = log_entry[ref_index: ref_index + 32]
                pattern = 'c_path=.*(\\w{32})'
                ref_output = None
                # once we have the reference number, we once again iterate through
                # the file and look to find where it appears we are looking for the
                # dataset id, which we locate using the above regex pattern
                for b in full_contents:
                    ref_index = b.find(reference_number)
                    if ref_index == -1:
                        matches = re.search(pattern, b)
                        if matches is not None:
                            ref_output = matches.group(1)
                            break
                if ref_output is not None:
                    dataset_id = ref_output
                    break

            # Now that we have the dataset id, we can derive hubmap_id, data_type,
            # and entity_type via the Entity Sdk
            hubmap_id = None
            data_type = None
            entity_type = None
            remove_record = False
            if dataset_id is not None:
                try:
                    entity_instance = EntitySdk(token=entity_token)
                    dataset = entity_instance.get_entity_by_id(dataset_id)
                    entity_type = dataset.entity_type
                    if entity_type == "Dataset":
                        data_type = dataset.data_types
                    else:
                        data_type = "N/A Upload"
                    hubmap_id = dataset.hubmap_id
                except hubmap_sdk.sdk_helper.HTTPException as e:
                    remove_record = True

            # hubmap public does not record dataset id, so for these transfers we
            # indicate these are unknown.
            public_id = "af603d86-eab9-4eec-bb1d-9d26556741bb"
            if (specific_usage["source_endpoint_id"][ind] == public_id
                    or specific_usage["destination_endpoint_id"][ind] == public_id):
                data_type = "Public: Unknown"
                hubmap_id = "Public: Unknown"
                entity_type = "Public: Unknown"
            if remove_record is False:
                data_types_column.append(data_type)
                hubmap_id_column.append(hubmap_id)
                entity_types_column.append(entity_type)
            else:
                rows_to_remove.append(ind)

        # Records that aren't found in entity api are likely removed, so they get
        # dropped from the data frame
        print(f'removing {len(rows_to_remove)} entries')
        rows_to_remove.reverse()
        for each in rows_to_remove:
            specific_usage.drop(specific_usage.index[each], inplace=True)
        print(f'specific_usage array has {len(specific_usage)} records after deletions')

        # Add these 3 columns to the data frame specific_usage
        specific_usage['data_type'] = data_types_column
        specific_usage['hubmap_id'] = hubmap_id_column
        specific_usage['entity_type'] = entity_types_column

        # Convert dataframe to json and output with the selected path/name
        specific_usage.to_json(path_or_buf=usage_output, orient='records')
        print(f'wrote {len(specific_usage)} records of usage data to {usage_output}')

    t_build_report = PythonOperator(
        task_id='build_report',
        python_callable=build_report,
        provide_context=True,
        op_kwargs={
            'crypt_auth_tok' : utils.encrypt_tok(airflow_conf.as_dict()
                                                 ['connections']['APP_CLIENT_SECRET']).decode(),
            }
        )
    
    t_create_tmpdir = CreateTmpDirOperator(task_id='create_tmpdir')
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id='cleanup_tmpdir')

    (dag
     >> t_create_tmpdir
     >> t_build_report
     >> t_cleanup_tmpdir
    )

