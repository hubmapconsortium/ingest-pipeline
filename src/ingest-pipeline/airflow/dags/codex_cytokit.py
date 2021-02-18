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
    get_cwltool_base_cmd,
    get_dataset_uuid,
    get_named_absolute_workflows,
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
    cwl_workflows = get_named_absolute_workflows(
        cytokit=Path(pipeline_name, 'pipeline.cwl'),
        sprm=Path('sprm', 'pipeline.cwl'),
        create_vis_symlink_archive=Path('create-vis-symlink-archive', 'pipeline.cwl'),
        ome_tiff_offsets=Path('portal-containers', 'ome-tiff-offsets.cwl'),
        sprm_to_json=Path('portal-containers', 'sprm-to-json.cwl'),
        sprm_to_anndata=Path('portal-containers', 'sprm-to-anndata.cwl'),
    )

    def build_dataset_name(**kwargs):
        return '{}__{}__{}'.format(dag.dag_id,
                                   kwargs['dag_run'].conf['parent_submission_id'],
                                   pipeline_name)


    prepare_cwl_cytokit = DummyOperator(task_id='prepare_cwl_cytokit')
    
    def build_cwltool_cwl_cytokit(**kwargs):
        ctx = kwargs['dag_run'].conf
        run_id = kwargs['run_id']
        tmpdir = utils.get_tmp_dir_path(run_id)
        print('tmpdir: ', tmpdir)
        data_dir = ctx['parent_lz_path']
        print('data_dir: ', data_dir)

        command = [
            *get_cwltool_base_cmd(tmpdir),
            cwl_workflows['cytokit'],
            '--gpus=0,1',
            '--data_dir',
            data_dir,
        ]

        return join_quote_command_str(command)


    t_build_cwl_cytokit = PythonOperator(
        task_id='build_cwl_cytokit',
        python_callable=build_cwltool_cwl_cytokit,
        provide_context=True,
        )


    t_pipeline_exec_cwl_cytokit = BashOperator(
        task_id='pipeline_exec_cwl_cytokit',
        queue=utils.map_queue_name('gpu000_q1'),
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        mkdir -p ${tmp_dir}/cwl_out ; \
        cd ${tmp_dir}/cwl_out ; \
        {{ti.xcom_pull(task_ids='build_cwl_cytokit')}} > $tmp_dir/session.log 2>&1 ; \
        echo $?
        """
    )


    t_maybe_keep_cwl_cytokit = BranchPythonOperator(
        task_id='maybe_keep_cwl_cytokit',
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs = {'next_op' : 'prepare_cwl_ome_tiff_offsets',
                     'bail_op' : 'set_dataset_error',
                     'test_op' : 'pipeline_exec_cwl_cytokit'}
        )


    prepare_cwl_ome_tiff_offsets = DummyOperator(task_id='prepare_cwl_ome_tiff_offsets')

    def build_cwltool_cmd_ome_tiff_offsets(**kwargs):
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
            cwl_workflows['ome_tiff_offsets'],
            '--input_dir',
            data_dir / 'output/extract/expressions/ome-tiff',
        ]

        return join_quote_command_str(command)


    t_build_cmd_ome_tiff_offsets = PythonOperator(
        task_id='build_cmd_ome_tiff_offsets',
        python_callable=build_cwltool_cmd_ome_tiff_offsets,
        provide_context=True,
        )


    t_pipeline_exec_cwl_ome_tiff_offsets = BashOperator(
        task_id='pipeline_exec_cwl_ome_tiff_offsets',
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        cd ${tmp_dir}/cwl_out ; \
        {{ti.xcom_pull(task_ids='build_cmd_ome_tiff_offsets')}} >> ${tmp_dir}/session.log 2>&1 ; \
        echo $?
        """
    )


    t_maybe_keep_cwl_ome_tiff_offsets = BranchPythonOperator(
        task_id='maybe_keep_cwl_ome_tiff_offsets',
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs = {'next_op' : 'prepare_cwl_sprm_to_json',
                     'bail_op' : 'set_dataset_error',
                     'test_op' : 'pipeline_exec_cwl_ome_tiff_offsets'}
        )


    prepare_cwl_sprm_to_json = DummyOperator(
        task_id='prepare_cwl_sprm_to_json'
        )
    
    def build_cwltool_cmd_sprm_to_json(**kwargs):
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
            cwl_workflows['sprm_to_json'],
            '--input_dir',
            data_dir / 'sprm_outputs',
        ]

        return join_quote_command_str(command)


    t_build_cmd_sprm_to_json = PythonOperator(
        task_id='build_cmd_sprm_to_json',
        python_callable=build_cwltool_cmd_sprm_to_json,
        provide_context=True,
        )


    t_pipeline_exec_cwl_sprm_to_json = BashOperator(
        task_id='pipeline_exec_cwl_sprm_to_json',
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        cd ${tmp_dir}/cwl_out ; \
        {{ti.xcom_pull(task_ids='build_cmd_sprm_to_json')}} >> ${tmp_dir}/session.log 2>&1 ; \
        echo $?
        """
    )


    t_maybe_keep_cwl_sprm_to_json = BranchPythonOperator(
        task_id='maybe_keep_cwl_sprm_to_json',
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs = {'next_op' : 'prepare_cwl_sprm_to_anndata',
                     'bail_op' : 'set_dataset_error',
                     'test_op' : 'pipeline_exec_cwl_sprm_to_json'}
        )
    
    prepare_cwl_sprm_to_anndata = DummyOperator(
        task_id='prepare_cwl_sprm_to_anndata'
        )
    
    def build_cwltool_cmd_sprm_to_anndata(**kwargs):
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
            cwl_workflows['sprm_to_anndata'],
            '--input_dir',
            data_dir / 'sprm_outputs',
        ]

        return join_quote_command_str(command)


    t_build_cmd_sprm_to_anndata = PythonOperator(
        task_id='build_cmd_sprm_to_anndata',
        python_callable=build_cwltool_cmd_sprm_to_anndata,
        provide_context=True,
        )


    t_pipeline_exec_cwl_sprm_to_anndata = BashOperator(
        task_id='pipeline_exec_cwl_sprm_to_anndata',
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        cd ${tmp_dir}/cwl_out ; \
        {{ti.xcom_pull(task_ids='build_cmd_sprm_to_anndata')}} >> ${tmp_dir}/session.log 2>&1 ; \
        echo $?
        """
    )

    t_maybe_keep_cwl_sprm_to_anndata = BranchPythonOperator(
        task_id='maybe_keep_cwl_sprm_to_anndata',
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs = {'next_op' : 'move_data',
                     'bail_op' : 'set_dataset_error',
                     'test_op' : 'pipeline_exec_cwl_sprm_to_anndata'}
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
            'pipeline_exec_cwl_cytokit',
            'pipeline_exec_cwl_ome_tiff_offsets',
            'pipeline_exec_cwl_sprm_to_json',
            'pipeline_exec_cwl_sprm_to_anndata',
            'move_data',
        ],
        cwl_workflows=list(cwl_workflows.values()),
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
     >> prepare_cwl_cytokit >> t_build_cwl_cytokit >> t_pipeline_exec_cwl_cytokit >> t_maybe_keep_cwl_cytokit
     >> prepare_cwl_ome_tiff_offsets >> t_build_cmd_ome_tiff_offsets >> t_pipeline_exec_cwl_ome_tiff_offsets >> t_maybe_keep_cwl_ome_tiff_offsets
     >> prepare_cwl_sprm_to_json >> t_build_cmd_sprm_to_json >> t_pipeline_exec_cwl_sprm_to_json >> t_maybe_keep_cwl_sprm_to_json
     >> prepare_cwl_sprm_to_anndata >> t_build_cmd_sprm_to_anndata >> t_pipeline_exec_cwl_sprm_to_anndata >> t_maybe_keep_cwl_sprm_to_anndata
     >> t_move_data >> t_expand_symlinks >> t_send_status >> t_join)
    t_maybe_keep_cwl_cytokit >> t_set_dataset_error
    t_maybe_keep_cwl_ome_tiff_offsets >> t_set_dataset_error
    t_maybe_keep_cwl_sprm_to_json >> t_set_dataset_error
    t_maybe_keep_cwl_sprm_to_anndata >> t_set_dataset_error
    t_set_dataset_error >> t_join
    t_join >> t_cleanup_tmpdir


