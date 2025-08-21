from datetime import datetime, timedelta
from pathlib import Path
import os
import pandas as pd
import json

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

import utils
from utils import (
    pythonop_set_dataset_state,
    get_parent_dataset_uuid,
    get_absolute_workflow,
    build_dataset_name as inner_build_dataset_name,
    get_uuid_for_error,
    join_quote_command_str,
    make_send_status_msg_function,
    get_tmp_dir_path,
    HMDAG,
    get_queue_resource,
    get_preserve_scratch_resource,
    pythonop_get_dataset_state,
    get_parent_dataset_path,
    pythonop_send_create_dataset,
    get_threads_resource,
    get_cwl_cmd_from_workflows,
)
from hubmap_operators.common_operators import (
    CleanupTmpDirOperator,
    CreateTmpDirOperator,
    JoinOperator,
    LogInfoOperator,
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
    "queue": get_queue_resource("object_x_analyte"),
    "on_failure_callback": utils.create_dataset_state_error_callback(get_uuid_for_error),
}


with HMDAG(
    "object_x_analyte",
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args=default_args,
    user_defined_macros={
        "tmp_dir_path": get_tmp_dir_path,
        # TODO: Update this to use a different resource.
        "preserve_scratch": get_preserve_scratch_resource("object_x_analyte"),
    },
) as dag:
    pipeline_name = "object_x_analyte"
    workflow_version = "1.0.0"
    workflow_description = ""

    cwl_workflows = [
        {
            "workflow_path": str(
                get_absolute_workflow(Path("portal-containers", "object-by-analyte-to-ui.cwl"))
            ),
            "documentation_url": "",
        },
    ]

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
        return "Object x Analyte"

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
            original_dataset_uuid = epic_get_original_dataset_uuid(**kwargs)
            kwargs["ti"].xcom_push(key="derived_dataset_uuid", value=original_dataset_uuid)
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

    # BEGIN - Object by Analyte to UI Region
    t_prepare_cwl_object_by_analyte_to_ui = EmptyOperator(
        task_id="prepare_cwl_object_by_analyte_to_ui"
    )

    def build_cwl_cmd_object_by_analyte_to_ui(**kwargs):
        run_id = kwargs["run_id"]
        ti = kwargs["ti"]
        data_dir = ti.xcom_pull(task_ids="create_or_use_dataset")
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)

        cwl_parameters = [
            {"parameter_name": "--outdir", "value": str(tmpdir / "cwl_out/hubmap_ui")},
        ]
        input_parameters = [
            {
                "parameter_name": "--input_dir",
                "value": str(Path(data_dir) / "derived/obj_by_analyte"),
            },
        ]

        command = get_cwl_cmd_from_workflows(
            cwl_workflows, 0, input_parameters, tmpdir, ti, cwl_parameters
        )
        return join_quote_command_str(command)

    t_build_cwl_cmd_object_by_analyte_to_ui = PythonOperator(
        task_id="build_cwl_cmd_object_by_analyte_to_ui",
        python_callable=build_cwl_cmd_object_by_analyte_to_ui,
        provide_context=True,
    )

    t_pipeline_exec_cwl_object_by_analyte_to_ui = BashOperator(
        task_id="pipeline_exec_cwl_object_by_analyte_to_ui",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        ds_dir="{{ti.xcom_pull(task_ids="create_or_use_dataset")}}" ; \
        mkdir -p ${tmp_dir}/cwl_out/hubmap_ui ; \
        {{ti.xcom_pull(task_ids='build_cwl_cmd_object_by_analyte_to_ui')}} >> $tmp_dir/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_maybe_keep_cwl_object_by_analyte_to_ui = BranchPythonOperator(
        task_id="maybe_keep_cwl_object_by_analyte_to_ui",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "move_data",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_object_by_analyte_to_ui",
        },
    )
    # END - Object by Analyte to UI Region

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
        original_dataset = pythonop_get_dataset_state(
            dataset_uuid_callable=epic_get_original_dataset_uuid, **kwargs
        )
        print(original_dataset)
        metadata = original_dataset.get("metadata", {})

        # Then we gather the metadata from the mudata transformation output
        # Always have to gather the metadata from the transformation
        data_dir = kwargs["ti"].xcom_pull(task_ids="create_or_use_dataset")
        try:
            output_metadata = json.load(
                open(
                    f"{data_dir}/extras/transformations/hubmap_ui/mudata-zarr/calculated_metadata.json"
                )
            )
        except FileNotFoundError as e:
            print(
                f"{data_dir}/extras/transformations/hubmap_ui/calculated_metadata.json does not exist."
            )
            output_metadata = {}

        metadata["calculated_metadata"] = output_metadata
        return metadata

    send_status_msg = make_send_status_msg_function(
        dag_file=__file__,
        retcode_ops=[
            "pipeline_exec_cwl_object_by_analyte_to_ui",
            "move_data",
        ],
        cwl_workflows=lambda **kwargs: kwargs["ti"].xcom_pull(
            key="cwl_workflows", task_ids="build_cwl_cmd_object_by_analyte_to_ui"
        ),
        uuid_src_task_id="create_or_use_dataset",
        metadata_fun=gather_metadata,
        workflow_description=workflow_description,
        workflow_version=workflow_version,
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
        >> prepare_cwl_object_by_analyte_to_ui
        >> t_build_cwl_cmd_object_by_analyte_to_ui
        >> t_pipeline_exec_cwl_object_by_analyte_to_ui
        >> t_maybe_keep_cwl_object_by_analyte_to_ui
        >> t_move_data
        >> t_send_status
        >> t_join
    )

    t_maybe_keep_cwl_object_by_analyte_to_ui >> t_set_dataset_error
    t_set_dataset_error >> t_join
    t_join >> t_cleanup_tmpdir
