from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from hubmap_operators.common_operators import (
    LogInfoOperator,
    JoinOperator,
    CreateTmpDirOperator,
    CleanupTmpDirOperator,
    SetDatasetProcessingOperator,
    MoveDataOperator
)

import utils
from utils import (
    get_absolute_workflows,
    get_cwltool_base_cmd,
    get_dataset_uuid,
    get_parent_dataset_uuid,
    get_uuid_for_error,
    join_quote_command_str,
    make_send_status_msg_function,
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
    'on_failure_callback': utils.create_dataset_state_error_callback(get_uuid_for_error)
}


with DAG('codex_cytokit', 
         schedule_interval=None, 
         is_paused_upon_creation=False, 
         default_args=default_args,
         max_active_runs=1,
         user_defined_macros={'tmp_dir_path' : utils.get_tmp_dir_path}
         ) as dag:

    pipeline_name = 'codex-pipeline'
    cwl_workflows = get_absolute_workflows(
        Path(pipeline_name, 'pipeline.cwl'),
        Path('portal-containers', 'ome-tiff-offsets.cwl'),
        Path('portal-containers', 'sprm-to-json.cwl'),
        Path('portal-containers', 'sprm-to-anndata.cwl'),
    )

    def build_dataset_name(**kwargs):
        return '{}__{}__{}'.format(dag.dag_id,
                                   kwargs['dag_run'].conf['parent_submission_id'],
                                   pipeline_name)


    prepare_cwl1 = DummyOperator(
        task_id='prepare_cwl1'
        )
    
    def build_cwltool_cmd1(**kwargs):
        ctx = kwargs['dag_run'].conf
        run_id = kwargs['run_id']
        tmpdir = utils.get_tmp_dir_path(run_id)
        print('tmpdir: ', tmpdir)
        data_dir = ctx['parent_lz_path']
        print('data_dir: ', data_dir)

        command = [
            *get_cwltool_base_cmd(tmpdir),
            cwl_workflows[0],
            '--gpus=0,1',
            '--data_dir',
            data_dir,
        ]

        return join_quote_command_str(command)


    t_build_cmd1 = PythonOperator(
        task_id='build_cmd1',
        python_callable=build_cwltool_cmd1,
        provide_context=True,
        )


    t_pipeline_exec_cwl1 = BashOperator(
        task_id='pipeline_exec_cwl1',
        queue=utils.map_queue_name('gpu000_q1'),
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        mkdir -p ${tmp_dir}/cwl_out ; \
        cd ${tmp_dir}/cwl_out ; \
        {{ti.xcom_pull(task_ids='build_cmd1')}} > $tmp_dir/session.log 2>&1 ; \
        echo $?
        """
    )


    t_maybe_keep_cwl1 = BranchPythonOperator(
        task_id='maybe_keep_cwl1',
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs = {'next_op' : 'prepare_cwl2',
                     'bail_op' : 'set_dataset_error',
                     'test_op' : 'pipeline_exec_cwl1'}
        )


    prepare_cwl2 = DummyOperator(
        task_id='prepare_cwl2'
        )
    
    def build_cwltool_cmd2(**kwargs):
        ctx = kwargs['dag_run'].conf
        run_id = kwargs['run_id']
        tmpdir = utils.get_tmp_dir_path(run_id)
        print('tmpdir: ', tmpdir)
        parent_data_dir = ctx['parent_lz_path']
        print('parent_data_dir: ', parent_data_dir)
        data_dir = tmpdir / 'cwl_out'
        print('data_dir: ', data_dir)

        command = [
            *get_cwltool_base_cmd(tmpdir),
            cwl_workflows[1],
            '--input_dir',
            data_dir / 'output/extract/expressions/ome-tiff',
        ]

        return join_quote_command_str(command)


    t_build_cmd2 = PythonOperator(
        task_id='build_cmd2',
        python_callable=build_cwltool_cmd2,
        provide_context=True,
        )


    t_pipeline_exec_cwl2 = BashOperator(
        task_id='pipeline_exec_cwl2',
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        cd ${tmp_dir}/cwl_out ; \
        {{ti.xcom_pull(task_ids='build_cmd2')}} >> ${tmp_dir}/session.log 2>&1 ; \
        echo $?
        """
    )


    t_maybe_keep_cwl2 = BranchPythonOperator(
        task_id='maybe_keep_cwl2',
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs = {'next_op' : 'prepare_cwl3',
                     'bail_op' : 'set_dataset_error',
                     'test_op' : 'pipeline_exec_cwl2'}
        )


    prepare_cwl3 = DummyOperator(
        task_id='prepare_cwl3'
        )
    
    def build_cwltool_cmd3(**kwargs):
        ctx = kwargs['dag_run'].conf
        run_id = kwargs['run_id']
        tmpdir = utils.get_tmp_dir_path(run_id)
        print('tmpdir: ', tmpdir)
        parent_data_dir = ctx['parent_lz_path']
        print('parent_data_dir: ', parent_data_dir)
        data_dir = tmpdir / 'cwl_out'  # This stage reads input from stage 1
        print('data_dir: ', data_dir)

        command = [
            *get_cwltool_base_cmd(tmpdir),
            cwl_workflows[2],
            '--input_dir',
            data_dir / 'sprm_outputs',
        ]

        return join_quote_command_str(command)


    t_build_cmd3 = PythonOperator(
        task_id='build_cmd3',
        python_callable=build_cwltool_cmd3,
        provide_context=True,
        )


    t_pipeline_exec_cwl3 = BashOperator(
        task_id='pipeline_exec_cwl3',
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        cd ${tmp_dir}/cwl_out ; \
        {{ti.xcom_pull(task_ids='build_cmd3')}} >> ${tmp_dir}/session.log 2>&1 ; \
        echo $?
        """
    )


    t_maybe_keep_cwl3 = BranchPythonOperator(
        task_id='maybe_keep_cwl3',
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs = {'next_op' : 'prepare_cwl4',
                     'bail_op' : 'set_dataset_error',
                     'test_op' : 'pipeline_exec_cwl3'}
        )
    
    prepare_cwl4 = DummyOperator(
        task_id='prepare_cwl4'
        )
    
    def build_cwltool_cmd4(**kwargs):
        ctx = kwargs['dag_run'].conf
        run_id = kwargs['run_id']
        tmpdir = utils.get_tmp_dir_path(run_id)
        print('tmpdir: ', tmpdir)
        parent_data_dir = ctx['parent_lz_path']
        print('parent_data_dir: ', parent_data_dir)
        data_dir = tmpdir / 'cwl_out'  # This stage reads input from stage 1
        print('data_dir: ', data_dir)

        command = [
            *get_cwltool_base_cmd(tmpdir),
            cwl_workflows[3],
            '--input_dir',
            data_dir / 'sprm_outputs',
        ]

        return join_quote_command_str(command)


    t_build_cmd4 = PythonOperator(
        task_id='build_cmd4',
        python_callable=build_cwltool_cmd3,
        provide_context=True,
        )


    t_pipeline_exec_cwl4 = BashOperator(
        task_id='pipeline_exec_cwl4',
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        cd ${tmp_dir}/cwl_out ; \
        {{ti.xcom_pull(task_ids='build_cmd4')}} >> ${tmp_dir}/session.log 2>&1 ; \
        echo $?
        """
    )

    t_maybe_keep_cwl4 = BranchPythonOperator(
        task_id='maybe_keep_cwl4',
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs = {'next_op' : 'move_data',
                     'bail_op' : 'set_dataset_error',
                     'test_op' : 'pipeline_exec_cwl4'}
        )


    t_send_create_dataset = PythonOperator(
        task_id='send_create_dataset',
        python_callable=utils.pythonop_send_create_dataset,
        provide_context=True,
        op_kwargs = {'parent_dataset_uuid_callable' : get_parent_dataset_uuid,
                     'http_conn_id' : 'ingest_api_connection',
                     'endpoint' : '/datasets/derived',
                     'dataset_name_callable' : build_dataset_name,
                     "dataset_types":["codex_cytokit"]
                     }
    )


    t_set_dataset_error = PythonOperator(
        task_id='set_dataset_error',
        python_callable=utils.pythonop_set_dataset_state,
        provide_context=True,
        trigger_rule='all_done',
        op_kwargs = {'dataset_uuid_callable' : get_dataset_uuid,
                     'http_conn_id' : 'ingest_api_connection',
                     'endpoint' : '/datasets/status',
                     'ds_state' : 'Error',
                     'message' : 'An error occurred in {}'.format(pipeline_name)
                     }
    )


    t_expand_symlinks = BashOperator(
        task_id='expand_symlinks',
        bash_command="""
        tmp_dir="{{tmp_dir_path(run_id)}}" ; \
        ds_dir="{{ti.xcom_pull(task_ids="send_create_dataset")}}" ; \
        groupname="{{conf.as_dict()['connections']['OUTPUT_GROUP_NAME']}}" ; \
        cd "$ds_dir" ; \
        tar -xf symlinks.tar ; \
        echo $?
        """
        )

    send_status_msg = make_send_status_msg_function(
        dag_file=__file__,
        retcode_ops=[
            'pipeline_exec_cwl1',
            'pipeline_exec_cwl2',
            'pipeline_exec_cwl3',
            'pipeline_exec_cwl4',
            'move_data',
        ],
        cwl_workflows=cwl_workflows,
    )
    t_send_status = PythonOperator(
        task_id='send_status_msg',
        python_callable=send_status_msg,
        provide_context=True
    )

    t_log_info = LogInfoOperator(task_id='log_info')
    t_join = JoinOperator(task_id='join')
    t_create_tmpdir = CreateTmpDirOperator(task_id='create_tmpdir')
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id='cleanup_tmpdir')
    t_set_dataset_processing = SetDatasetProcessingOperator(task_id='set_dataset_processing')
    t_move_data = MoveDataOperator(task_id='move_data')

    (dag >> t_log_info >> t_create_tmpdir
     >> t_send_create_dataset >> t_set_dataset_processing
     >> prepare_cwl1 >> t_build_cmd1 >> t_pipeline_exec_cwl1 >> t_maybe_keep_cwl1
     >> prepare_cwl2 >> t_build_cmd2 >> t_pipeline_exec_cwl2 >> t_maybe_keep_cwl2
     >> prepare_cwl3 >> t_build_cmd3 >> t_pipeline_exec_cwl3 >> t_maybe_keep_cwl3
     >> prepare_cwl4 >> t_build_cmd4 >> t_pipeline_exec_cwl4 >> t_maybe_keep_cwl4
     >> t_move_data >> t_expand_symlinks >> t_send_status >> t_join)
    t_maybe_keep_cwl1 >> t_set_dataset_error
    t_maybe_keep_cwl2 >> t_set_dataset_error
    t_maybe_keep_cwl3 >> t_set_dataset_error
    t_maybe_keep_cwl4 >> t_set_dataset_error
    t_set_dataset_error >> t_join
    t_join >> t_cleanup_tmpdir


