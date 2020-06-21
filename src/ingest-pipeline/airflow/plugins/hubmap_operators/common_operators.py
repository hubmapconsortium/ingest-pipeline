#! /usr/bin/env python

from pprint import pprint

from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults

from utils import (
    get_dataset_uuid,
    pythonop_set_dataset_state,
    pythonop_trigger_target,
)

class LogInfoOperator(PythonOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        print('args follow')
        pprint(args)
        print('kwargs follow')
        pprint(kwargs)
        super().__init__(python_callable=pythonop_trigger_target,
                         provide_context=True,
                         *args, **kwargs)
    

class JoinOperator(DummyOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(trigger_rule='one_success', *args, **kwargs)


class CreateTmpDirOperator(BashOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(
            bash_command='mkdir -p {{tmp_dir_path(run_id)}}',
            provide_context=True,
            trigger_rule='all_success',
            *args, **kwargs)


class CleanupTmpDirOperator(BashOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(
            bash_command="""
            tmp_dir="{{tmp_dir_path(run_id)}}" ; \
            ds_dir="{{ti.xcom_pull(task_ids="send_create_dataset")}}" ; \
            mv "$tmp_dir/session.log" "$ds_dir"  && rm -r "$tmp_dir"
            """,
            trigger_rule='all_success',
            *args, **kwargs
            )


class SetDatasetProcessingOperator(PythonOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(python_callable=pythonop_set_dataset_state, 
                         op_kwargs={
                             'dataset_uuid_callable': get_dataset_uuid,
                             'http_conn_id': 'ingest_api_connection',
                             'endpoint': '/datasets/status'
                         },
                         *args, **kwargs)
    

class MoveDataOperator(BashOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(
            bash_command="""
            tmp_dir="{{tmp_dir_path(run_id)}}" ; \
            ds_dir="{{ti.xcom_pull(task_ids="send_create_dataset")}}" ; \
            groupname="{{conf.as_dict()['connections']['OUTPUT_GROUP_NAME']}}" ; \
            pushd "$ds_dir" ; \
            sudo chown airflow . ; \
            sudo chgrp $groupname . ; \
            popd ; \
            mv "$tmp_dir"/cwl_out/* "$ds_dir" >> "$tmp_dir/session.log" 2>&1 ; \
            echo $?
            """,
            provide_context=True,
            *args, **kwargs)
