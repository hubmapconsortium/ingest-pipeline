#! /usr/bin/env python

# from pprint import pprint

# from airflow.operators.bash_operator import BashOperator
from airflow.operators.bash import BashOperator
# from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dummy import DummyOperator
# from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import PythonOperator
# from airflow.utils.decorators import apply_defaults

from utils import (
    get_dataset_uuid,
    pythonop_set_dataset_state,
    pythonop_trigger_target,
)


class LogInfoOperator(PythonOperator):
    # @apply_defaults
    def __init__(self, **kwargs):
        # print('kwargs follow')
        # pprint(kwargs)
        super().__init__(python_callable=pythonop_trigger_target,
                         provide_context=True,
                         **kwargs)
    

class JoinOperator(DummyOperator):
    # @apply_defaults
    def __init__(self, **kwargs):
        super().__init__(trigger_rule='one_success', **kwargs)


class CreateTmpDirOperator(BashOperator):
    # @apply_defaults
    def __init__(self, **kwargs):
        super().__init__(
            bash_command='mkdir -p {{tmp_dir_path(run_id)}}',
            trigger_rule='all_success',
            **kwargs)


class CleanupTmpDirOperator(BashOperator):
    # @apply_defaults
    def __init__(self, **kwargs):
        super().__init__(
            bash_command="""
            tmp_dir="{{tmp_dir_path(run_id)}}" ; \
            if [ -e "$tmp_dir/session.log" ] ; then \
              ds_dir="{{ti.xcom_pull(task_ids='send_create_dataset')}}" ; \
              cp "$tmp_dir/session.log" "$ds_dir"  && echo "copied session.log" ; \
            fi ; \
            echo rmscratch is $rmscratch ; \
            if [ "$rmscratch" = true ] ; then \
              rm -r "$tmp_dir" ; \
            else \
              echo "scratch directory was preserved" ; \
            fi
            """,
            env={'rmscratch': '{{"true" if preserve_scratch is defined and not preserve_scratch else "false"}}'},
            trigger_rule='all_success',
            **kwargs
            )


class SetDatasetProcessingOperator(PythonOperator):
    # @apply_defaults
    def __init__(self, **kwargs):
        super().__init__(python_callable=pythonop_set_dataset_state, 
                         provide_context=True,
                         op_kwargs={
                             'dataset_uuid_callable': get_dataset_uuid,
                         },
                         **kwargs)
    

class MoveDataOperator(BashOperator):
    # @apply_defaults
    def __init__(self, **kwargs):
        print(f'Kwargs {kwargs}')
        if 'populate_tmpdir' in kwargs['task_id']:
            command = """
            tmp_dir="{{tmp_dir_path(run_id)}}" ; \
            ds_dir="{{ti.xcom_pull(task_ids="send_create_dataset", key="previous_revision_path")}}" ; \
            pushd "$ds_dir" ; \
            popd ; \
            mkdir "$tmp_dir"/cwl_out/ ; \
            pushd "$tmp_dir"/cwl_out/ ; \
            popd ; \
            cp -r "$ds_dir"/* "$tmp_dir/cwl_out/" >> "$tmp_dir/session.log" 2>&1 ; \
            echo $?
            """
        else:
            command = """
            tmp_dir="{{tmp_dir_path(run_id)}}" ; \
            ds_dir="{{ti.xcom_pull(task_ids="send_create_dataset")}}" ; \
            pushd "$ds_dir" ; \
            popd ; \
            mv "$tmp_dir"/cwl_out/* "$ds_dir" >> "$tmp_dir/session.log" 2>&1 ; \
            echo $?
            """
        super().__init__(
            bash_command=command, **kwargs)
