from datetime import datetime, timedelta
from pprint import pprint

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from hubmap_operators.common_operators import (
    LogInfoOperator,
    JoinOperator,
    CreateTmpDirOperator,
    CleanupTmpDirOperator,
    SetDatasetProcessingOperator,
)

import utils

from utils import (
    get_dataset_uuid,
    get_parent_dataset_uuids_list,
    get_parent_data_dir,
    build_dataset_name as inner_build_dataset_name,
    get_previous_revision_uuid,
    get_uuid_for_error,
    join_quote_command_str,
    make_send_status_msg_function,
    get_tmp_dir_path
)

default_args = {
    'owner': 'hubmap',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email': ['joel.welling@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'xcom_push': True,
    'queue': utils.map_queue_name('general'),
    'on_failure_callback': utils.create_dataset_state_error_callback(get_uuid_for_error),
}

with DAG(
        'devtest_step2',
        schedule_interval=None,
        is_paused_upon_creation=False,
        default_args=default_args,
        max_active_runs=1,
        user_defined_macros={'tmp_dir_path': get_tmp_dir_path},
) as dag:
    pipeline_name = 'devtest-step2-pipeline'

    def build_dataset_name(**kwargs):
        return inner_build_dataset_name(dag.dag_id, pipeline_name, **kwargs)

    def build_cwltool_cmd1(**kwargs):
        ctx = kwargs['dag_run'].conf
        run_id = kwargs['run_id']
        tmpdir = get_tmp_dir_path(run_id)
        tmp_subdir = tmpdir / 'cwl_out'
        data_dir = get_parent_data_dir(**kwargs)

        try:
            delay_sec = int(ctx['metadata']['delay_sec'])
        except ValueError:
            print("Could not parse delay_sec "
                  "{} ; defaulting to 30 sec".format(ctx['metadata']['delay_sec']))
            delay_sec = 30
        for fname in ctx['metadata']['files_to_copy']:
            print(fname)

        commands = [
            [f'tmp_dir={tmpdir}'],
            ['sleep', delay_sec],
            ['cd', data_dir],
            ['mkdir', '-p', tmp_subdir],
        ]

        if ctx['metadata']['files_to_copy']:
            commands.append(['cp', *ctx['metadata']['files_to_copy'], tmp_subdir])

        print('command list:')
        pprint(commands)

        command_strs = [join_quote_command_str(command) for command in commands]
        command_str = ' ; '.join(command_strs)
        print('overall command_str:', command_str)
        return command_str

    t_build_cmd1 = PythonOperator(
        task_id='build_cmd1',
        python_callable=build_cwltool_cmd1,
        provide_context=True,
    )

    t_pipeline_exec = BashOperator(
        task_id='pipeline_exec',
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        {{ti.xcom_pull(task_ids='build_cmd1')}} > $tmp_dir/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_maybe_keep_cwl1 = BranchPythonOperator(
        task_id='maybe_keep_cwl1',
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            'next_op': 'move_data',
            'bail_op': 'set_dataset_error',
            'test_op': 'pipeline_exec',
        },
    )

    t_send_create_dataset = PythonOperator(
        task_id='send_create_dataset',
        python_callable=utils.pythonop_send_create_dataset,
        provide_context=True,
        op_kwargs={
            'parent_dataset_uuid_callable': get_parent_dataset_uuids_list,
            'previous_revision_uuid_callable': get_previous_revision_uuid,
            'http_conn_id': 'ingest_api_connection',
            'dataset_name_callable': build_dataset_name,
            'dataset_types': ["devtest"],
        },
    )

    t_set_dataset_error = PythonOperator(
        task_id='set_dataset_error',
        python_callable=utils.pythonop_set_dataset_state,
        provide_context=True,
        trigger_rule='all_done',
        op_kwargs={
            'dataset_uuid_callable': get_dataset_uuid,
            'http_conn_id': 'ingest_api_connection',
            'ds_state': 'Error',
            'message': f'An error occurred in {pipeline_name}',
        },
    )

    t_move_data = BashOperator(
        task_id='move_data',
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
    )

    send_status_msg = make_send_status_msg_function(
        dag_file=__file__,
        retcode_ops=['pipeline_exec', 'move_data'],
        cwl_workflows=[],
    )
    t_send_status = PythonOperator(
        task_id='send_status_msg',
        python_callable=send_status_msg,
        provide_context=True,
    )

    t_log_info = LogInfoOperator(task_id='log_info')
    t_join = JoinOperator(task_id='join')
    t_create_tmpdir = CreateTmpDirOperator(task_id='create_tmpdir')
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id='cleanup_tmpdir')
    t_set_dataset_processing = SetDatasetProcessingOperator(task_id='set_dataset_processing')

    (dag >> t_log_info >> t_create_tmpdir
     >> t_send_create_dataset >> t_set_dataset_processing
     >> t_build_cmd1 >> t_pipeline_exec >> t_maybe_keep_cwl1
     >> t_move_data >> t_send_status >> t_join)
    t_maybe_keep_cwl1 >> t_set_dataset_error >> t_join
    t_join >> t_cleanup_tmpdir
