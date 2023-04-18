from pathlib import Path
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
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
    get_parent_dataset_uuids_list,
    get_parent_data_dir,
    build_dataset_name as inner_build_dataset_name,
    get_previous_revision_uuid,
    get_uuid_for_error,
    join_quote_command_str,
    make_send_status_msg_function,
    get_tmp_dir_path,
    HMDAG,
    get_queue_resource,
    get_preserve_scratch_resource,
    get_instance_type,
    get_environment_instance
)

from aws_utils import (
    create_instance,
    terminate_instance
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
    'queue': get_queue_resource('codex_cytokit'),
    'on_failure_callback': utils.create_dataset_state_error_callback(get_uuid_for_error)
}

with HMDAG('codex_cytokit',
           schedule_interval=None,
           is_paused_upon_creation=False,
           default_args=default_args,
           user_defined_macros={
               'tmp_dir_path': get_tmp_dir_path,
               'preserve_scratch': get_preserve_scratch_resource('codex_cytokit'),
           }) as dag:

    def start_new_environment(**kwargs):
        uuid = kwargs['run_id']
        instance_id = create_instance(uuid, f'Airflow {get_environment_instance()} Worker',
                                      get_instance_type(kwargs['dag_run'].conf['dag_id']))
        if instance_id is None:
            return 1
        else:
            kwargs['ti'].xcom_push(key='instance_id', value=instance_id)
            return 0


    t_initialize_environment = PythonOperator(
        task_id='initialize_environment',
        python_callable=start_new_environment,
        provide_context=True,
        op_kwargs={
        }
    )

    pipeline_name = 'codex-pipeline'
    steps_dir = Path(pipeline_name) / 'steps'
    cwl_workflows = get_named_absolute_workflows(
        illumination_first_stitching=steps_dir / 'illumination_first_stitching.cwl',
        cytokit=steps_dir / 'run_cytokit.cwl',
        ometiff_second_stitching=steps_dir / 'ometiff_second_stitching.cwl',
        sprm=Path('sprm', 'pipeline.cwl'),
        create_vis_symlink_archive=Path('create-vis-symlink-archive', 'pipeline.cwl'),
        ome_tiff_pyramid=Path('ome-tiff-pyramid', 'pipeline.cwl'),
        ome_tiff_offsets=Path('portal-containers', 'ome-tiff-offsets.cwl'),
        sprm_to_json=Path('portal-containers', 'sprm-to-json.cwl'),
        sprm_to_anndata=Path('portal-containers', 'sprm-to-anndata.cwl'),
    )

    def build_dataset_name(**kwargs):
        return inner_build_dataset_name(dag.dag_id, pipeline_name, **kwargs)

    prepare_cwl_illumination_first_stitching = DummyOperator(
        task_id='prepare_cwl_illumination_first_stitching',
    )

    def build_cwltool_cwl_illumination_first_stitching(**kwargs):
        run_id = kwargs['run_id']
        tmpdir = get_tmp_dir_path(run_id)
        print('tmpdir: ', tmpdir)
        data_dir = get_parent_data_dir(**kwargs)
        print('data_dir: ', data_dir)

        command = [
            *get_cwltool_base_cmd(tmpdir),
            cwl_workflows['illumination_first_stitching'],
            '--gpus=0,1',
            '--data_dir',
            data_dir,
        ]

        return join_quote_command_str(command)

    t_build_cwl_illumination_first_stitching = PythonOperator(
        task_id='build_cwl_illumination_first_stitching',
        python_callable=build_cwltool_cwl_illumination_first_stitching,
        provide_context=True,
    )

    t_pipeline_exec_cwl_illumination_first_stitching = BashOperator(
        task_id='pipeline_exec_cwl_illumination_first_stitching',
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        mkdir -p ${tmp_dir}/cwl_out ; \
        cd ${tmp_dir}/cwl_out ; \
        {{ti.xcom_pull(task_ids='build_cwl_illumination_first_stitching')}} > $tmp_dir/session.log 2>&1 ; \
        echo $?
        """
    )

    t_maybe_keep_cwl_illumination_first_stitching = BranchPythonOperator(
        task_id='maybe_keep_cwl_illumination_first_stitching',
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            'next_op': 'prepare_cwl_cytokit',
            'bail_op': 'set_dataset_error',
            'test_op': 'pipeline_exec_cwl_illumination_first_stitching',
        }
    )

    def start_new_gpu_environment(**kwargs):
        uuid = get_dataset_uuid(**kwargs)
        instance_id = create_instance(uuid, f'GPU {get_environment_instance()} Worker',
                                      get_instance_type(kwargs['dag_run'].conf['dag_id'],
                                                        kwargs['dag_run'].conf['task_id']))
        if instance_id is None:
            return 1
        else:
            kwargs['ti'].xcom_push(key='instance_id', value=instance_id)
            return 0


    t_initialize_environment_gpu = PythonOperator(
        task_id='initialize_environment_gpu',
        python_callable=start_new_gpu_environment,
        provide_context=True,
        op_kwargs={
        }
    )

    prepare_cwl_cytokit = DummyOperator(task_id='prepare_cwl_cytokit')

    def build_cwltool_cwl_cytokit(**kwargs):
        run_id = kwargs['run_id']
        tmpdir = get_tmp_dir_path(run_id)
        print('tmpdir: ', tmpdir)
        data_dir = tmpdir / 'cwl_out'
        print('data_dir: ', data_dir)

        command = [
            *get_cwltool_base_cmd(tmpdir),
            cwl_workflows['cytokit'],
            '--data_dir',
            data_dir / 'new_tiles',
            '--yaml_config',
            data_dir / 'experiment.yaml',
        ]

        return join_quote_command_str(command)

    t_build_cwl_cytokit = PythonOperator(
        task_id='build_cwl_cytokit',
        python_callable=build_cwltool_cwl_cytokit,
        provide_context=True,
        )

    t_pipeline_exec_cwl_cytokit = BashOperator(
        task_id='pipeline_exec_cwl_cytokit',
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        mkdir -p ${tmp_dir}/cwl_out ; \
        cd ${tmp_dir}/cwl_out ; \
        {{ti.xcom_pull(task_ids='build_cwl_cytokit')}} >> $tmp_dir/session.log 2>&1 ; \
        echo $?
        """
    )

    t_maybe_keep_cwl_cytokit = BranchPythonOperator(
        task_id='maybe_keep_cwl_cytokit',
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            'next_op': 'prepare_cwl_ometiff_second_stitching',
            'bail_op': 'set_dataset_error',
            'test_op': 'pipeline_exec_cwl_cytokit',
        }
    )

    def terminate_new_gpu_environment(**kwargs):
        instance_id = kwargs['ti'].xcom_pull(key='instance_id', task_ids="initialize_environment_gpu")
        if instance_id is None:
            return 1
        else:
            uuid = get_dataset_uuid(**kwargs)
            terminate_instance(instance_id, uuid)
        return 0


    t_terminate_environment_gpu = PythonOperator(
        task_id='terminate_environment_gpu',
        python_callable=terminate_new_gpu_environment,
        provide_context=True,
        op_kwargs={
        }
    )

    prepare_cwl_ometiff_second_stitching = DummyOperator(
        task_id='prepare_cwl_ometiff_second_stitching',
    )

    def build_cwltool_cwl_ometiff_second_stitching(**kwargs):
        run_id = kwargs['run_id']
        tmpdir = get_tmp_dir_path(run_id)
        print('tmpdir: ', tmpdir)
        data_dir = tmpdir / 'cwl_out'
        print('data_dir: ', data_dir)

        command = [
            *get_cwltool_base_cmd(tmpdir),
            cwl_workflows['ometiff_second_stitching'],
            '--cytokit_config',
            data_dir / 'experiment.yaml',
            '--cytokit_output',
            data_dir / 'cytokit',
            '--slicing_pipeline_config',
            data_dir / 'pipelineConfig.json'
        ]

        return join_quote_command_str(command)

    t_build_cwl_ometiff_second_stitching = PythonOperator(
        task_id='build_cwl_ometiff_second_stitching',
        python_callable=build_cwltool_cwl_ometiff_second_stitching,
        provide_context=True,
    )

    t_pipeline_exec_cwl_ometiff_second_stitching = BashOperator(
        task_id='pipeline_exec_cwl_ometiff_second_stitching',
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        mkdir -p ${tmp_dir}/cwl_out ; \
        cd ${tmp_dir}/cwl_out ; \
        {{ti.xcom_pull(task_ids='build_cwl_ometiff_second_stitching')}} >> $tmp_dir/session.log 2>&1 ; \
        echo $?
        """
    )

    t_maybe_keep_cwl_ometiff_second_stitching = BranchPythonOperator(
        task_id='maybe_keep_cwl_ometiff_second_stitching',
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            'next_op': 'prepare_cwl_sprm',
            'bail_op': 'set_dataset_error',
            'test_op': 'pipeline_exec_cwl_ometiff_second_stitching',
        }
    )

    t_delete_internal_pipeline_files = BashOperator(
        task_id='delete_internal_pipeline_files',
        bash_command="""\
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        cd ${tmp_dir}/cwl_out ; \
        rm -rf cytokit new_tiles
        """
    )

    prepare_cwl_sprm = DummyOperator(task_id='prepare_cwl_sprm')

    def build_cwltool_cmd_sprm(**kwargs):
        run_id = kwargs['run_id']
        tmpdir = get_tmp_dir_path(run_id)
        print('tmpdir: ', tmpdir)
        parent_data_dir = get_parent_data_dir(**kwargs)
        print('parent_data_dir: ', parent_data_dir)
        data_dir = tmpdir / 'cwl_out'
        print('data_dir: ', data_dir)

        command = [
            *get_cwltool_base_cmd(tmpdir),
            cwl_workflows['sprm'],
            '--enable_manhole',
            '--image_dir',
            data_dir / 'pipeline_output/expr',
            '--mask_dir',
            data_dir / 'pipeline_output/mask',
        ]

        return join_quote_command_str(command)


    t_build_cmd_sprm = PythonOperator(
        task_id='build_cmd_sprm',
        python_callable=build_cwltool_cmd_sprm,
        provide_context=True,
        )

    t_pipeline_exec_cwl_sprm = BashOperator(
        task_id='pipeline_exec_cwl_sprm',
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        cd ${tmp_dir}/cwl_out ; \
        {{ti.xcom_pull(task_ids='build_cmd_sprm')}} >> ${tmp_dir}/session.log 2>&1 ; \
        echo $?
        """
    )

    t_maybe_keep_cwl_sprm = BranchPythonOperator(
        task_id='maybe_keep_cwl_sprm',
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={'next_op': 'prepare_cwl_create_vis_symlink_archive',
                   'bail_op': 'set_dataset_error',
                   'test_op': 'pipeline_exec_cwl_sprm'}
        )

    prepare_cwl_create_vis_symlink_archive = DummyOperator(
        task_id='prepare_cwl_create_vis_symlink_archive',
    )

    def build_cwltool_cmd_create_vis_symlink_archive(**kwargs):
        run_id = kwargs['run_id']
        tmpdir = get_tmp_dir_path(run_id)
        print('tmpdir: ', tmpdir)
        parent_data_dir = get_parent_data_dir(**kwargs)
        print('parent_data_dir: ', parent_data_dir)
        data_dir = tmpdir / 'cwl_out'
        print('data_dir: ', data_dir)

        command = [
            *get_cwltool_base_cmd(tmpdir),
            cwl_workflows['create_vis_symlink_archive'],
            '--ometiff_dir',
            data_dir / 'pipeline_output',
            '--sprm_output',
            data_dir / 'sprm_outputs',
        ]

        return join_quote_command_str(command)


    t_build_cmd_create_vis_symlink_archive = PythonOperator(
        task_id='build_cmd_create_vis_symlink_archive',
        python_callable=build_cwltool_cmd_create_vis_symlink_archive,
        provide_context=True,
        )

    t_pipeline_exec_cwl_create_vis_symlink_archive = BashOperator(
        task_id='pipeline_exec_cwl_create_vis_symlink_archive',
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        cd ${tmp_dir}/cwl_out ; \
        {{ti.xcom_pull(task_ids='build_cmd_create_vis_symlink_archive')}} >> ${tmp_dir}/session.log 2>&1 ; \
        echo $?
        """
    )

    t_maybe_keep_cwl_create_vis_symlink_archive = BranchPythonOperator(
        task_id='maybe_keep_cwl_create_vis_symlink_archive',
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={'next_op': 'prepare_cwl_ome_tiff_pyramid',
                   'bail_op': 'set_dataset_error',
                   'test_op': 'pipeline_exec_cwl_create_vis_symlink_archive'}
        )

    prepare_cwl_ome_tiff_pyramid = DummyOperator(task_id='prepare_cwl_ome_tiff_pyramid')

    def build_cwltool_cwl_ome_tiff_pyramid(**kwargs):
        run_id = kwargs['run_id']

        # tmpdir is temp directory in /hubmap-tmp
        tmpdir = get_tmp_dir_path(run_id)
        print('tmpdir: ', tmpdir)

        # data directory is the stitched images, which are found in tmpdir
        data_dir = get_parent_data_dir(**kwargs)
        print('data_dir: ', data_dir)

        # this is the call to the CWL
        command = [
            *get_cwltool_base_cmd(tmpdir),
            '--relax-path-checks',
            cwl_workflows['ome_tiff_pyramid'],
            '--ometiff_directory',
            '.',
        ]
        return join_quote_command_str(command)

    t_build_cmd_ome_tiff_pyramid = PythonOperator(
        task_id='build_cwl_ome_tiff_pyramid',
        python_callable=build_cwltool_cwl_ome_tiff_pyramid,
        provide_context=True,
        )

    t_pipeline_exec_cwl_ome_tiff_pyramid = BashOperator(
        task_id='pipeline_exec_cwl_ome_tiff_pyramid',
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        mkdir -p ${tmp_dir}/cwl_out ; \
        cd ${tmp_dir}/cwl_out ; \
        {{ti.xcom_pull(task_ids='build_cwl_ome_tiff_pyramid')}} >> $tmp_dir/session.log 2>&1 ; \
        echo $?
        """
        )

    t_maybe_keep_cwl_ome_tiff_pyramid = BranchPythonOperator(
        task_id='maybe_keep_cwl_ome_tiff_pyramid',
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={'next_op': 'prepare_cwl_ome_tiff_offsets',
                   'bail_op': 'set_dataset_error',
                   'test_op': 'pipeline_exec_cwl_ome_tiff_pyramid'}
        )

    prepare_cwl_ome_tiff_offsets = DummyOperator(task_id='prepare_cwl_ome_tiff_offsets')

    def build_cwltool_cmd_ome_tiff_offsets(**kwargs):
        run_id = kwargs['run_id']
        tmpdir = get_tmp_dir_path(run_id)
        print('tmpdir: ', tmpdir)
        parent_data_dir = get_parent_data_dir(**kwargs)
        print('parent_data_dir: ', parent_data_dir)
        data_dir = tmpdir / 'cwl_out'
        print('data_dir: ', data_dir)

        command = [
            *get_cwltool_base_cmd(tmpdir),
            cwl_workflows['ome_tiff_offsets'],
            '--input_dir',
            data_dir / 'ometiff-pyramids',
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
        op_kwargs={'next_op': 'prepare_cwl_sprm_to_json',
                   'bail_op': 'set_dataset_error',
                   'test_op': 'pipeline_exec_cwl_ome_tiff_offsets'}
        )

    prepare_cwl_sprm_to_json = DummyOperator(
        task_id='prepare_cwl_sprm_to_json'
        )

    def build_cwltool_cmd_sprm_to_json(**kwargs):
        run_id = kwargs['run_id']
        tmpdir = get_tmp_dir_path(run_id)
        print('tmpdir: ', tmpdir)
        parent_data_dir = get_parent_data_dir(**kwargs)
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
        op_kwargs={'next_op': 'prepare_cwl_sprm_to_anndata',
                   'bail_op': 'set_dataset_error',
                   'test_op': 'pipeline_exec_cwl_sprm_to_json'}
        )

    prepare_cwl_sprm_to_anndata = DummyOperator(
        task_id='prepare_cwl_sprm_to_anndata'
        )

    def build_cwltool_cmd_sprm_to_anndata(**kwargs):
        run_id = kwargs['run_id']
        tmpdir = get_tmp_dir_path(run_id)
        print('tmpdir: ', tmpdir)
        parent_data_dir = get_parent_data_dir(**kwargs)
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
        op_kwargs={'next_op': 'move_data',
                   'bail_op': 'set_dataset_error',
                   'test_op': 'pipeline_exec_cwl_sprm_to_anndata'}
        )

    t_send_create_dataset = PythonOperator(
        task_id='send_create_dataset',
        python_callable=utils.pythonop_send_create_dataset,
        provide_context=True,
        op_kwargs={'parent_dataset_uuid_callable': get_parent_dataset_uuids_list,
                   'previous_revision_uuid_callable': get_previous_revision_uuid,
                   'http_conn_id': 'ingest_api_connection',
                   'dataset_name_callable': build_dataset_name,
                   'dataset_types': ['codex_cytokit']
                   }
    )

    t_set_dataset_error = PythonOperator(
        task_id='set_dataset_error',
        python_callable=utils.pythonop_set_dataset_state,
        provide_context=True,
        trigger_rule='all_done',
        op_kwargs={'dataset_uuid_callable': get_dataset_uuid,
                   'ds_state': 'Error',
                   'message': 'An error occurred in {}'.format(pipeline_name)
                   }
    )

    t_expand_symlinks = BashOperator(
        task_id='expand_symlinks',
        bash_command="""
        tmp_dir='{{tmp_dir_path(run_id)}}' ; \
        ds_dir='{{ti.xcom_pull(task_ids='send_create_dataset')}}' ; \
        groupname='{{conf.as_dict()['connections']['OUTPUT_GROUP_NAME']}}' ; \
        cd '$ds_dir' ; \
        tar -xf symlinks.tar ; \
        echo $?
        """
        )

    send_status_msg = make_send_status_msg_function(
        dag_file=__file__,
        retcode_ops=[
            'pipeline_exec_cwl_illumination_first_stitching',
            'pipeline_exec_cwl_cytokit',
            'pipeline_exec_cwl_ometiff_second_stitching',
            'pipeline_exec_cwl_sprm',
            'pipeline_exec_cwl_create_vis_symlink_archive',
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

    def terminate_new_environment(**kwargs):
        instance_id = kwargs['ti'].xcom_pull(key='instance_id', task_ids="initialize_environment")
        if instance_id is None:
            return 1
        else:
            uuid = kwargs['run_id']
            terminate_instance(instance_id, uuid)
        return 0


    t_terminate_environment = PythonOperator(
        task_id='terminate_environment',
        python_callable=terminate_new_environment,
        provide_context=True,
        op_kwargs={
        }
    )

    t_log_info = LogInfoOperator(task_id='log_info')
    t_join = JoinOperator(task_id='join')
    t_create_tmpdir = CreateTmpDirOperator(task_id='create_tmpdir')
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id='cleanup_tmpdir')
    t_set_dataset_processing = SetDatasetProcessingOperator(task_id='set_dataset_processing')
    t_move_data = MoveDataOperator(task_id='move_data')

    (
        t_log_info
        >> t_create_tmpdir
        >> t_send_create_dataset
        >> t_set_dataset_processing
        >> t_initialize_environment

        >> prepare_cwl_illumination_first_stitching
        >> t_build_cwl_illumination_first_stitching
        >> t_pipeline_exec_cwl_illumination_first_stitching
        >> t_maybe_keep_cwl_illumination_first_stitching

        >> t_initialize_environment_gpu
        >> prepare_cwl_cytokit
        >> t_build_cwl_cytokit
        >> t_pipeline_exec_cwl_cytokit
        >> t_maybe_keep_cwl_cytokit
        >> t_terminate_environment_gpu

        >> prepare_cwl_ometiff_second_stitching
        >> t_build_cwl_ometiff_second_stitching
        >> t_pipeline_exec_cwl_ometiff_second_stitching
        >> t_maybe_keep_cwl_ometiff_second_stitching

        >> prepare_cwl_sprm
        >> t_build_cmd_sprm
        >> t_pipeline_exec_cwl_sprm
        >> t_maybe_keep_cwl_sprm

        >> prepare_cwl_create_vis_symlink_archive
        >> t_build_cmd_create_vis_symlink_archive
        >> t_pipeline_exec_cwl_create_vis_symlink_archive
        >> t_maybe_keep_cwl_create_vis_symlink_archive

        >> prepare_cwl_ome_tiff_pyramid
        >> t_build_cmd_ome_tiff_pyramid
        >> t_pipeline_exec_cwl_ome_tiff_pyramid
        >> t_maybe_keep_cwl_ome_tiff_pyramid

        >> prepare_cwl_ome_tiff_offsets
        >> t_build_cmd_ome_tiff_offsets
        >> t_pipeline_exec_cwl_ome_tiff_offsets
        >> t_maybe_keep_cwl_ome_tiff_offsets

        >> prepare_cwl_sprm_to_json
        >> t_build_cmd_sprm_to_json
        >> t_pipeline_exec_cwl_sprm_to_json
        >> t_maybe_keep_cwl_sprm_to_json

        >> prepare_cwl_sprm_to_anndata
        >> t_build_cmd_sprm_to_anndata
        >> t_pipeline_exec_cwl_sprm_to_anndata
        >> t_maybe_keep_cwl_sprm_to_anndata

        >> t_move_data
        >> t_expand_symlinks
        >> t_send_status
        >> t_join
        >> t_terminate_environment
    )
    t_pipeline_exec_cwl_ometiff_second_stitching >> t_delete_internal_pipeline_files
    t_maybe_keep_cwl_illumination_first_stitching >> t_set_dataset_error
    t_maybe_keep_cwl_cytokit >> t_terminate_environment_gpu >> t_set_dataset_error
    t_maybe_keep_cwl_ometiff_second_stitching >> t_set_dataset_error
    t_maybe_keep_cwl_sprm >> t_set_dataset_error
    t_maybe_keep_cwl_create_vis_symlink_archive >> t_set_dataset_error
    t_maybe_keep_cwl_ome_tiff_pyramid >> t_set_dataset_error
    t_maybe_keep_cwl_ome_tiff_offsets >> t_set_dataset_error
    t_maybe_keep_cwl_sprm_to_json >> t_set_dataset_error
    t_maybe_keep_cwl_sprm_to_anndata >> t_set_dataset_error
    t_set_dataset_error >> t_join
    t_join >> t_cleanup_tmpdir
    t_cleanup_tmpdir >> t_terminate_environment
