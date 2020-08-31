import os
import json
from pathlib import Path
from pprint import pprint
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.http_hook import HttpHook

#these are the hubmap common operators that are used in all DAGS
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
    decrypt_tok,
    find_pipeline_manifests,
    get_cwltool_base_cmd,
    get_absolute_workflows,
    get_dataset_uuid,
    get_parent_dataset_uuid,
    get_uuid_for_error,
    join_quote_command_str,
    localized_assert_json_matches_schema as assert_json_matches_schema,
)

# Passed directly to the pipeline
DOWNSAMPLE_TYPE = 'LINEAR'

# after running this DAG you should have on disk
# 1. 1 OME.TIFF pyramid per OME.TIFF in the original dataset
# 2. 1 .N5 file per OME.TIFF in the original dataset
# 3. 1 JSON file
default_args = {
    'owner': 'hubmap',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email': ['icaoberg@psc.edu'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'xcom_push': True,
    'queue': utils.map_queue_name('general'),
    'on_failure_callback': utils.create_dataset_state_error_callback(get_uuid_for_error)
}

with DAG('ometiff_pyramid',
         schedule_interval=None,
         is_paused_upon_creation=False,
         default_args=default_args,
         max_active_runs=1,
         user_defined_macros={'tmp_dir_path' : utils.get_tmp_dir_path}
         ) as dag:

    # does the name need to match the filename?
    pipeline_name = 'ometiff_pyramid'

    cwl_workflows = get_absolute_workflows(
        # this workflow creates the image pyramid
        Path('ome-tiff-pyramid', 'pipeline.cwl'),
        # this workflow computes the offsets
        Path('portal-containers', 'ome-tiff-offsets.cwl'),
    )

    def build_dataset_name(**kwargs):
        return '{}__{}__{}'.format(dag.dag_id,
                                   kwargs['dag_run'].conf['parent_submission_id'],
                                   pipeline_name)

    # CWL1 - pipeline.cwl
    prepare_cwl1 = DummyOperator(
        task_id='prepare_cwl1'
        )

    #print useful info and build command line
    def build_cwltool_cmd1(**kwargs):
        ctx = kwargs['dag_run'].conf
        run_id = kwargs['run_id']

        #tmpdir is temp directory in /hubmap-tmp
        tmpdir = Path(utils.get_tmp_dir_path(run_id))
        print('tmpdir: ', tmpdir)

        #data directory is input directory in /hubmap-data
        data_dir = ctx['parent_lz_path']
        print('data_dir: ', data_dir)

        # TODO: implement this check however is appropriate
        is_ims = False

        #this is the call to the CWL
        command = [
            *get_cwltool_base_cmd(tmpdir),
            cwl_workflows[0],
            '--ometiff_directory',
            data_dir,
        ]
        if is_ims:
            command.append('--downsample_type')
            command.append(DOWNSAMPLE_TYPE)

        return join_quote_command_str(command)

    t_build_cmd1 = PythonOperator(
        task_id='build_cmd1',
        python_callable=build_cwltool_cmd1
        )

    t_pipeline_exec_cwl1 = BashOperator(
        task_id='pipeline_exec_cwl1',
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        mkdir -p ${tmp_dir}/cwl_out ; \
        cd ${tmp_dir}/cwl_out ; \
        {{ti.xcom_pull(task_ids='build_cmd1')}} > $tmp_dir/session.log 2>&1 ; \
        echo $?
        """
    )

    #CWL2
    prepare_cwl2 = DummyOperator(
        task_id='prepare_cwl2'
        )

    #print useful info and build command line
    def build_cwltool_cmd2(**kwargs):
        ctx = kwargs['dag_run'].conf
        run_id = kwargs['run_id']

        #tmpdir is temp directory in /hubmap-tmp
        tmpdir = Path(utils.get_tmp_dir_path(run_id))
        print('tmpdir: ', tmpdir)

        #get data directory
        parent_data_dir = ctx['parent_lz_path']
        print('parent_data_dir: ', parent_data_dir)
        data_dir = os.path.join(tmpdir, 'cwl_out', 'ometiff-pyramids')  # This stage reads input from stage 1
        print('data_dir: ', data_dir)

        #this is the call to the CWL
        command = [
            *get_cwltool_base_cmd(tmpdir),
            cwl_workflows[1],
            '--input_directory',
            './ometiff-pyramids',
        ]

        return join_quote_command_str(command)

    t_build_cmd2 = PythonOperator(
        task_id='build_cmd2',
        python_callable=build_cwltool_cmd2
        )

    t_pipeline_exec_cwl2 = BashOperator(
        task_id='pipeline_exec_cwl2',
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        cd ${tmp_dir}/cwl_out ; \
        {{ti.xcom_pull(task_ids='build_cmd2')}} >> $tmp_dir/session.log 2>&1 ; \
        echo $?
        """
    )

    #next_op if true, bail_op if false. test_op returns value for testing.
    t_maybe_keep_cwl2 = BranchPythonOperator(
        task_id='maybe_keep_cwl2',
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs = {'next_op' : 'move_data',
                     'bail_op' : 'set_dataset_error',
                     'test_op' : 'pipeline_exec_cwl2'}
        )

    #Others
    t_send_create_dataset = PythonOperator(
        task_id='send_create_dataset',
        python_callable=utils.pythonop_send_create_dataset,
        provide_context=True,
        op_kwargs = {'parent_dataset_uuid_callable' : get_parent_dataset_uuid,
                     'http_conn_id' : 'ingest_api_connection',
                     'endpoint' : '/datasets/derived',
                     'dataset_name_callable' : build_dataset_name,
                     "dataset_types":["image_pyramid"]
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

    #next_op if true, bail_op if false. test_op returns value for testing.
    t_maybe_keep_cwl1 = BranchPythonOperator(
        task_id='maybe_keep_cwl1',
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs = {'next_op' : 'prepare_cwl2',
                     'bail_op' : 'set_dataset_error',
                     'test_op' : 'pipeline_exec_cwl1'}
        )

    # t_expand_symlinks = BashOperator(
    #     task_id='expand_symlinks',
    #     bash_command="""
    #     tmp_dir="{{tmp_dir_path(run_id)}}" ; \
    #     ds_dir="{{ti.xcom_pull(task_ids="send_create_dataset")}}" ; \
    #     groupname="{{conf.as_dict()['connections']['OUTPUT_GROUP_NAME']}}" ; \
    #     cd "$ds_dir" ; \
    #     tar -xf symlinks.tar ; \
    #     echo $?
    #     """
    #     )

    def send_status_msg(**kwargs):
        retcode_ops = ['pipeline_exec_cwl1', 'pipeline_exec_cwl2', 'move_data']
        retcodes = [int(kwargs['ti'].xcom_pull(task_ids=op))
                    for op in retcode_ops]
        print('retcodes: ', {k:v for k, v in zip(retcode_ops, retcodes)})
        success = all([rc == 0 for rc in retcodes])
        derived_dataset_uuid = kwargs['ti'].xcom_pull(key='derived_dataset_uuid',
                                                      task_ids="send_create_dataset")
        ds_dir = kwargs['ti'].xcom_pull(task_ids='send_create_dataset')
        http_conn_id='ingest_api_connection'
        endpoint='/datasets/status'
        method='PUT'
        crypt_auth_tok = kwargs['dag_run'].conf['crypt_auth_tok']
        headers={
            'authorization' : 'Bearer ' + decrypt_tok(crypt_auth_tok.encode()),
            'content-type' : 'application/json'}
        print('headers:')
        print(headers)  # reduce visibility of auth_tok
        extra_options=[]

        http = HttpHook(method,
                        http_conn_id=http_conn_id)

        if success:
            md = {}
            files_for_provenance = [__file__, *cwl_workflows]

            if 'dag_provenance' in kwargs['dag_run'].conf:
                md['dag_provenance'] = kwargs['dag_run'].conf['dag_provenance'].copy()
                new_prv_dct = utils.get_git_provenance_dict(files_for_provenance)
                md['dag_provenance'].update(new_prv_dct)
            else:
                dag_prv = (kwargs['dag_run'].conf['dag_provenance_list']
                           if 'dag_provenance_list' in kwargs['dag_run'].conf
                           else [])
                dag_prv.extend(utils.get_git_provenance_list(files_for_provenance))
                md['dag_provenance_list'] = dag_prv
            manifest_files = find_pipeline_manifests(cwl_workflows)
            md.update(utils.get_file_metadata_dict(ds_dir,
                                                   utils.get_tmp_dir_path(kwargs['run_id']),
                                                   manifest_files))
            try:
                assert_json_matches_schema(md, 'dataset_metadata_schema.yml')
                data = {'dataset_id' : derived_dataset_uuid,
                        'status' : 'QA',
                        'message' : 'The process ran',
                        'metadata': md}
            except AssertionError as e:
                print('invalid metadata follows:')
                pprint(md)
                data = {'dataset_id' : derived_dataset_uuid,
                        'status' : 'Error',
                        'message' : 'internal error; schema violation: {}'.format(e),
                        'metadata': {}}
        else:
            log_fname = os.path.join(utils.get_tmp_dir_path(kwargs['run_id']),
                                     'session.log')
            with open(log_fname, 'r') as f:
                err_txt = '\n'.join(f.readlines())
            data = {'dataset_id' : derived_dataset_uuid,
                    'status' : 'Invalid',
                    'message' : err_txt}
        print('data: ')
        pprint(data)

        response = http.run(endpoint,
                            json.dumps(data),
                            headers,
                            extra_options)

        print('response: ')
        pprint(response.json())

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

    # DAG
    (dag >> t_log_info >> t_create_tmpdir
     >> t_send_create_dataset >> t_set_dataset_processing
     >> prepare_cwl1 >> t_build_cmd1 >> t_pipeline_exec_cwl1
     >> t_maybe_keep_cwl1 >> prepare_cwl2 >> t_build_cmd2
     >> t_pipeline_exec_cwl2 >> t_maybe_keep_cwl2
     >> t_move_data >> t_send_status >> t_join)
    t_maybe_keep_cwl1 >> t_set_dataset_error
    t_maybe_keep_cwl2 >> t_set_dataset_error
    t_set_dataset_error >> t_join
    t_join >> t_cleanup_tmpdir
