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
    def __init__(self, **kwargs):
        #print('kwargs follow')
        #pprint(kwargs)
        super().__init__(python_callable=pythonop_trigger_target,
                         provide_context=True,
                         **kwargs)
    

class JoinOperator(DummyOperator):
    @apply_defaults
    def __init__(self, **kwargs):
        super().__init__(trigger_rule='one_success', **kwargs)


class CreateTmpDirOperator(BashOperator):
    @apply_defaults
    def __init__(self, **kwargs):
        super().__init__(
            bash_command='mkdir -p {{tmp_dir_path(run_id)}}',
            trigger_rule='all_success',
            **kwargs)


class CleanupTmpDirOperator(BashOperator):
    @apply_defaults
    def __init__(self, **kwargs):
        super().__init__(
            bash_command="""
            tmp_dir="{{tmp_dir_path(run_id)}}" ; \
            if [ -e "$tmp_dir/session.log" ] ; then
              ds_dir="{{ti.xcom_pull(task_ids="send_create_dataset")}}" ; \
              mv "$tmp_dir/session.log" "$ds_dir"  && echo rm -r "$tmp_dir" ; \
            else \
              echo rm -r "$tmp_dir" ; \
            fi
            """,
            trigger_rule='all_success',
            **kwargs
            )


class SetDatasetProcessingOperator(PythonOperator):
    @apply_defaults
    def __init__(self, **kwargs):
        super().__init__(python_callable=pythonop_set_dataset_state, 
                         provide_context=True,
                         op_kwargs={
                             'dataset_uuid_callable': get_dataset_uuid,
                         },
                         **kwargs)
    

class MoveDataOperator(BashOperator):
    @apply_defaults
    def __init__(self, **kwargs):
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
            **kwargs)
