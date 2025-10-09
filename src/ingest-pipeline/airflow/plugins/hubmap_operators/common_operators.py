#! /usr/bin/env python

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

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


class JoinOperator(EmptyOperator):
    # @apply_defaults
    def __init__(self, **kwargs):
        if "trigger_rule" in kwargs:
            trigger_rule = kwargs.pop("trigger_rule")
        else:
            trigger_rule = "all_done"
        super().__init__(trigger_rule=trigger_rule, **kwargs)


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
        if "trigger_rule" in kwargs:
            trigger_rule = kwargs.pop("trigger_rule")
        else:
            trigger_rule = 'all_success'
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
            trigger_rule=trigger_rule,
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
            chmod -R +w "$tmp_dir/cwl_out" 2>&1 ; \
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


class MoveDataDownstreamOperator(BashOperator):
    # @apply_defaults
    def __init__(self, **kwargs):
        command = """
            tmp_dir="{{dag_run.conf.tmp_dir}}" ; \
            ds_dir="{{ti.xcom_pull(task_ids="send_create_dataset")}}" ; \
            pushd "$ds_dir" ; \
            popd ; \
            mv "$tmp_dir"/cwl_out/* "$ds_dir" >> "$tmp_dir/session.log" 2>&1 ; \
            echo $?
        """
        super().__init__(
            bash_command=command, **kwargs)
