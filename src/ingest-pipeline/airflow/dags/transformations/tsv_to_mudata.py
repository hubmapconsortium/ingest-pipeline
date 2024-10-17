from datetime import datetime, timedelta
from pathlib import Path
import os
import pandas as pd
import json

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator

import utils
from utils import (
    pythonop_set_dataset_state,
    get_cwltool_base_cmd,
    get_parent_dataset_uuid,
    get_absolute_workflows,
    build_dataset_name as inner_build_dataset_name,
    get_uuid_for_error,
    join_quote_command_str,
    make_send_status_msg_function,
    get_tmp_dir_path,
    HMDAG,
    get_queue_resource,
    get_preserve_scratch_resource,
    pythonop_get_dataset_state,
    get_dataset_uuid,
    get_parent_dataset_path,
    pythonop_send_create_dataset,
)
from hubmap_operators.common_operators import (
    CleanupTmpDirOperator,
    CreateTmpDirOperator,
    JoinOperator,
    LogInfoOperator,
    MoveDataOperator,
    SetDatasetProcessingOperator,
)


default_args = {
    "owner": "hubmap",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 1),
    "email": ["joel.welling@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "xcom_push": True,
    "queue": get_queue_resource("tsv_to_mudata"),
    "on_failure_callback": utils.create_dataset_state_error_callback(get_uuid_for_error),
}


with HMDAG(
    "tsv_to_mudata",
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args=default_args,
    user_defined_macros={
        "tmp_dir_path": get_tmp_dir_path,
        # TODO: Update this to use a different resource.
        "preserve_scratch": get_preserve_scratch_resource("pas_ftu_segmentation"),
    },
) as dag:
    pipeline_name = "tsv_to_mudata"

    cwl_workflows = get_absolute_workflows(
        Path("epic-obj-csv-to-mudata", "pipeline.cwl"),
        Path("portal-containers", "seg-mudata-to-zarr.cwl"),
        Path("ome-tiff-pyramid", "pipeline.cwl"),
        Path("portal-containers", "ome-tiff-offsets.cwl"),
    )

    t_log_info = LogInfoOperator(task_id="log_info")

    def epic_get_parent_dataset_uuids(**kwargs):
        return pythonop_get_dataset_state(
            dataset_uuid_callable=get_parent_dataset_uuid, **kwargs
        ).get("parent_dataset_uuid_list", [])

    def epic_get_uuid_for_revision(**kwargs):
        status = pythonop_get_dataset_state(
            dataset_uuid_callable=get_parent_dataset_uuid, **kwargs
        ).get("status")

        # Equivalent to grabbing the submission_id
        return epic_get_original_dataset_uuid(**kwargs) if status.lower() == "published" else None

    def epic_get_original_dataset_uuid(**kwargs):
        return get_parent_dataset_uuid(**kwargs)

    def epic_get_dataset_uuid_to_process(**kwargs):
        # Derived dataset uuid will either be original dataset_uuid
        # or the dataset uuid from creating a new dataset
        return kwargs["ti"].xcom_pull(key="derived_dataset_uuid")

    def epic_get_dataset_type(**kwargs):
        return "Segmentation Mask"

    def build_dataset_name(**kwargs):
        return inner_build_dataset_name(dag.dag_id, pipeline_name, **kwargs)

    def create_or_use_dataset(**kwargs):
        # Check for transformation existence
        current_dataset_path = str(get_parent_dataset_path(**kwargs))
        kwargs["ti"].xcom_push(key="original_epic_dataset_path", value=current_dataset_path)
        if (Path(current_dataset_path) / "extras" / "transformations").exists():
            # Need to get the current dataset status to see whether we need to set the epic_get_revision_uuid
            abs_path = pythonop_send_create_dataset(
                parent_dataset_uuid_callable=epic_get_parent_dataset_uuids,
                previous_revision_uuid_callable=epic_get_uuid_for_revision,
                http_conn_id="ingest_api_connection",
                dataset_name_callable=build_dataset_name,
                dataset_type_callable=epic_get_dataset_type,
                creation_action="External Process",
                **kwargs,
            )
            return abs_path
        else:
            kwargs["ti"].xcom_push(
                key="derived_dataset_uuid", value=epic_get_original_dataset_uuid
            )
            return current_dataset_path

    t_create_or_use_dataset = PythonOperator(
        task_id="create_or_use_dataset",
        python_callable=create_or_use_dataset,
        provide_context=True,
    )

    t_create_tmpdir = CreateTmpDirOperator(task_id="create_tmpdir")

    # Copy data over to new dataset (excluding extras/transformations directory)
    # If we're using the existing dataset, then it's just a copy into itself.
    # We set $src_dir/ for the copy b/c otherwise it'll try to copy the directory
    # as a subdirectory in the $dest_dir
    t_copy_data = BashOperator(
        task_id="copy_data",
        bash_command="""
            tmp_dir="{{tmp_dir_path(run_id)}}" ; \
            src_dir="{{ti.xcom_pull(task_ids="create_or_use_dataset", key="original_epic_dataset_path")}}" ; \
            dest_dir="{{ti.xcom_pull(task_ids="create_or_use_dataset")}}" ; \
            echo "$dest_dir" ; \
            popd ; \
            rsync --exclude "extras/transformations/" -r "$src_dir/" "$dest_dir" >> "$tmp_dir/session.log" 2>&1 ; \
            echo $?
        """,
    )

    t_set_dataset_processing = PythonOperator(
        python_callable=pythonop_set_dataset_state,
        provide_context=True,
        op_kwargs={
            "dataset_uuid_callable": epic_get_dataset_uuid_to_process,
        },
        task_id="set_dataset_processing",
    )

    # Convert the obj by feature files to TSV
    def convert_obj_by_feature_to_tsv(**kwargs):
        data_dir = kwargs["ti"].xcom_pull(task_ids="create_or_use_dataset")
        for root, dirs, files in os.walk(f"{data_dir}"):
            for file in files:
                file_path = os.path.join(root, file)
                file_name, file_ext = os.path.splitext(file)

                # If it is not an obj by feature file skip it.
                if "objects" not in file_name:
                    continue

                if file_ext == ".xlsx":
                    df = pd.read_excel(file_path)
                elif file_ext == ".csv":
                    df = pd.read_csv(file_path)
                else:
                    continue

                output_file = os.path.join(root, f"{file_name}.tsv")
                df.to_csv(output_file, sep="\t", index=False)

    t_convert_obj_by_feature_to_tsv = PythonOperator(
        task_id="convert_obj_by_feature_to_tsv",
        python_callable=convert_obj_by_feature_to_tsv,
        provide_context=True,
    )

    # BEGIN - TSV -> MuData Region
    prepare_cwl_tsv_to_mudata = DummyOperator(task_id="prepare_cwl_tsv_to_mudata")

    def build_cwl_cmd_tsv_to_mudata(**kwargs):
        run_id = kwargs["run_id"]
        ti = kwargs["ti"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)
        data_dir = ti.xcom_pull(task_ids="create_or_use_dataset")
        print("data_dir: ", data_dir)

        workflow = cwl_workflows[0]

        command = [
            *get_cwltool_base_cmd(tmpdir),
            # "--singularity",
            workflow,
            "--data_dir",
            data_dir,
        ]

        return join_quote_command_str(command)

    t_build_cwl_cmd_tsv_to_mudata = PythonOperator(
        task_id="build_cwl_cmd_tsv_to_mudata",
        python_callable=build_cwl_cmd_tsv_to_mudata,
        provide_context=True,
    )

    t_pipeline_exec_cwl_tsv_to_mudata = BashOperator(
        task_id="pipeline_exec_cwl_tsv_to_mudata",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        mkdir -p ${tmp_dir}/cwl_out ; \
        cd ${tmp_dir}/cwl_out ; \
        {{ti.xcom_pull(task_ids='build_cwl_cmd_tsv_to_mudata')}} > $tmp_dir/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_maybe_keep_cwl_tsv_to_mudata = BranchPythonOperator(
        task_id="maybe_keep_cwl_tsv_to_mudata",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "build_cwl_cmd_seg_mudata_to_zarr",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_tsv_to_mudata",
        },
    )

    def build_cwl_cmd_seg_mudata_to_zarr(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)

        workflow = cwl_workflows[1]

        command = [
            *get_cwltool_base_cmd(tmpdir),
            workflow,
            "--input_dir",
            # This pipeline invocation runs in a 'hubmap_ui' subdirectory,
            # so use the parent directory as input
            "..",
        ]

        return join_quote_command_str(command)

    t_build_cwl_cmd_seg_mudata_to_zarr = PythonOperator(
        task_id="build_cwl_cmd_seg_mudata_to_zarr",
        python_callable=build_cwl_cmd_seg_mudata_to_zarr,
        provide_context=True,
    )

    t_pipeline_exec_cwl_seg_mudata_to_zarr = BashOperator(
        task_id="pipeline_exec_cwl_seg_mudata_to_zarr",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        ds_dir="{{ti.xcom_pull(task_ids="create_or_use_dataset")}}" ; \
        cd "$tmp_dir"/cwl_out ; \
        mkdir -p hubmap_ui ; \
        cd hubmap_ui ; \
        {{ti.xcom_pull(task_ids='build_cwl_cmd_seg_mudata_to_zarr')}} >> $tmp_dir/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_maybe_keep_cwl_seg_mudata_to_zarr = BranchPythonOperator(
        task_id="maybe_keep_cwl_mudata_to_zarr",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "build_cwl_cmd_ome_tiff_pyramid",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_seg_mudata_to_zarr",
        },
    )
    # END - TSV -> MuData Region

    # BEGIN - Image Pyramid Region
    # print useful info and build command line
    def build_cwl_cmd_ome_tiff_pyramid(**kwargs):
        run_id = kwargs["run_id"]
        ti = kwargs["ti"]

        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)
        data_dir = ti.xcom_pull(task_ids="create_or_use_dataset")
        print("data_dir: ", data_dir)

        workflow = cwl_workflows[2]
        # this is the call to the CWL
        command = [
            *get_cwltool_base_cmd(tmpdir),
            workflow,
            "--ometiff_directory",
            data_dir,
        ]

        return join_quote_command_str(command)

    t_build_cwl_cmd_ome_tiff_pyramid = PythonOperator(
        task_id="build_cwl_cmd_ome_tiff_pyramid",
        python_callable=build_cwl_cmd_ome_tiff_pyramid,
        provide_context=True,
    )

    t_pipeline_exec_cwl_ome_tiff_pyramid = BashOperator(
        task_id="pipeline_exec_cwl_ome_tiff_pyramid",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        mkdir -p ${tmp_dir}/cwl_out ; \
        cd ${tmp_dir}/cwl_out ; \
        {{ti.xcom_pull(task_ids='build_cwl_cmd_ome_tiff_pyramid')}} > $tmp_dir/session.log 2>&1 ; \
        echo $?
        """,
    )

    # print useful info and build command line
    def build_cwl_cmd_ome_tiff_offsets(**kwargs):
        run_id = kwargs["run_id"]

        # tmpdir is temp directory in /hubmap-tmp
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)

        workflow = cwl_workflows[3]

        # this is the call to the CWL
        command = [
            *get_cwltool_base_cmd(tmpdir),
            workflow,
            "--input_directory",
            "./ometiff-pyramids",
        ]

        return join_quote_command_str(command)

    t_build_cwl_cmd_ome_tiff_offsets = PythonOperator(
        task_id="build_cwl_cmd_ome_tiff_offsets",
        python_callable=build_cwl_cmd_ome_tiff_offsets,
        provide_context=True,
    )

    t_pipeline_exec_cwl_ome_tiff_offsets = BashOperator(
        task_id="pipeline_exec_cwl_ome_tiff_offsets",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        cd ${tmp_dir}/cwl_out ; \
        {{ti.xcom_pull(task_ids='build_cwl_cmd_ome_tiff_offsets')}} >> $tmp_dir/session.log 2>&1 ; \
        echo $?
        """,
    )

    # next_op if true, bail_op if false. test_op returns value for testing.
    t_maybe_keep_cwl_ome_tiff_offsets = BranchPythonOperator(
        task_id="maybe_keep_cwl_ome_tiff_offsets",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "move_data",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_ome_tiff_offsets",
        },
    )
    # END - Image Pyramid Region

    # Move data back to dataset_uuid
    t_move_data = BashOperator(
        task_id="move_data",
        bash_command="""
            tmp_dir="{{tmp_dir_path(run_id)}}" ; \
            ds_dir="{{ti.xcom_pull(task_ids="create_or_use_dataset")}}/extras/transformations" ; \
            mkdir "$ds_dir" ; \
            pushd "$ds_dir" ; \
            popd ; \
            mv "$tmp_dir"/cwl_out/* "$ds_dir" >> "$tmp_dir/session.log" 2>&1 ; \
            echo $?
        """,
    )

    def gather_metadata(**kwargs):
        # First we gather the metadata from the parent dataset
        # We only need to copy the metadata if we created a new dataset
        metadata = {}
        if epic_get_original_dataset_uuid(**kwargs) != epic_get_dataset_uuid_to_process(**kwargs):
            original_dataset = pythonop_get_dataset_state(
                dataset_uuid_callable=epic_get_original_dataset_uuid, **kwargs
            )
            print(original_dataset)
            metadata |= original_dataset.get("metadata", {})

        # Then we gather the metadata from the mudata transformation output
        # Always have to gather the metadata from the transformation
        data_dir = kwargs["ti"].xcom_pull(task_ids="create_or_use_dataset")
        output_metadata = json.load(open(f"{data_dir}/extras/transformations/metadata.json"))
        metadata["calculated_metadata"] = output_metadata
        return metadata

    send_status_msg = make_send_status_msg_function(
        dag_file=__file__,
        retcode_ops=[
            "pipeline_exec_cwl_tsv_to_mudata",
            "pipeline_exec_cwl_seg_mudata_to_zarr",
            "pipeline_exec_cwl_ome_tiff_pyramid",
            "pipeline_exec_cwl_ome_tiff_offsets",
            "move_data",
        ],
        cwl_workflows=cwl_workflows,
        uuid_src_task_id="create_or_use_dataset",
        metadata_fun=gather_metadata,
    )

    t_send_status = PythonOperator(
        task_id="send_status_msg", python_callable=send_status_msg, provide_context=True
    )

    t_set_dataset_error = PythonOperator(
        task_id="set_dataset_error",
        python_callable=pythonop_set_dataset_state,
        provide_context=True,
        trigger_rule="all_done",
        op_kwargs={
            "dataset_uuid_callable": epic_get_dataset_uuid_to_process,
            "ds_state": "Error",
            "message": "An error occurred in {}".format(pipeline_name),
        },
    )

    t_join = JoinOperator(task_id="join")
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id="cleanup_tmpdir")

    (
        t_log_info
        >> t_create_or_use_dataset
        >> t_create_tmpdir
        >> t_copy_data
        >> t_set_dataset_processing
        >> t_convert_obj_by_feature_to_tsv
        >> prepare_cwl_tsv_to_mudata
        >> t_build_cwl_cmd_tsv_to_mudata
        >> t_pipeline_exec_cwl_tsv_to_mudata
        >> t_maybe_keep_cwl_tsv_to_mudata
        >> t_build_cwl_cmd_seg_mudata_to_zarr
        >> t_pipeline_exec_cwl_seg_mudata_to_zarr
        >> t_maybe_keep_cwl_seg_mudata_to_zarr
        >> t_build_cwl_cmd_ome_tiff_pyramid
        >> t_pipeline_exec_cwl_ome_tiff_pyramid
        >> t_build_cwl_cmd_ome_tiff_offsets
        >> t_pipeline_exec_cwl_ome_tiff_offsets
        >> t_maybe_keep_cwl_ome_tiff_offsets
        >> t_move_data
        >> t_send_status
        >> t_join
    )

    t_maybe_keep_cwl_tsv_to_mudata >> t_set_dataset_error
    t_maybe_keep_cwl_seg_mudata_to_zarr >> t_set_dataset_error
    t_maybe_keep_cwl_ome_tiff_offsets >> t_set_dataset_error
    t_set_dataset_error >> t_join
    t_join >> t_cleanup_tmpdir
