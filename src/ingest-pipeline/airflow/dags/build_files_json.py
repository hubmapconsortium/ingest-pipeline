from pathlib import Path
from pprint import pprint

from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.configuration import conf as airflow_conf
from datetime import datetime
from airflow import DAG

from hubmap_operators.common_operators import (
    CreateTmpDirOperator,
    LogInfoOperator,
)

from utils import (
    HMDAG,
    get_queue_resource,
    get_preserve_scratch_resource,
    get_tmp_dir_path,
    encrypt_tok,
    pythonop_build_dataset_lists,
    get_file_metadata_dict,
    find_pipeline_manifests,
)


def get_uuid_for_error(**kwargs):
    """
    Return the uuid for the derived dataset if it exists, and of the parent dataset otherwise.
    """
    return None


default_args = {
    "start_date": datetime(2019, 1, 1),
}

with HMDAG(
    "build_files_json",
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args=default_args,
    user_defined_macros={
        "tmp_dir_path": get_tmp_dir_path,
        "preserve_scratch": get_preserve_scratch_resource("build_files_json"),
    },
) as dag:

    def build_dataset_lists(**kwargs):
        print("dag_run conf follows:")
        pprint(kwargs["dag_run"].conf)
        return pythonop_build_dataset_lists(**kwargs)

    t_build_dataset_lists = PythonOperator(
        task_id="build_dataset_lists",
        python_callable=build_dataset_lists,
        provide_context=True,
        queue=get_queue_resource("build_files_json"),
        op_kwargs={
            "crypt_auth_tok": encrypt_tok(
                airflow_conf.as_dict()["connections"]["APP_CLIENT_SECRET"]
            ).decode(),
        },
    )

    def emit_files_json(**kwargs):

        # Only processed datasets have files: information
        uuid_list = kwargs["dag_run"].conf.get("processed_datasets", [])
        rslt = {}
        pipeline_file_manifests = find_pipeline_manifests(kwargs["dag_run"].conf["cwl_workflows"])
        for uuid in uuid_list:
            def uuid_callable(**kwargs):
                return uuid
            ds_state = pythonop_get_dataset_state(dataset_uuid_callable=uuid_callable)
            lz_path = ds_state["local_directory_full_path"]
            file_metadata_dict = get_file_metadata_dict(
                lz_path,
                ".",  # not used since max_in_line_files is < 0
                pipeline_file_manifests,
                max_in_line_files = -1
            )
            rslt[uuid] = file_metadata_dict
        json_file_path = Path(get_tmp_dir_path(kwargs["run_id"])) / "uuid_files.json"
        with open(json_file_path, "w") as f:
            json.dump(rslt, f)

    t_emit_files_json = PythonOperator(
        task_id="emit_files_json",
        python_callable=emit_files_json,
        queue=get_queue_resource("build_files_json"),
        provide_context=True,
    )

    t_log_info = LogInfoOperator(task_id="log_info")
    t_create_tmpdir = CreateTmpDirOperator(task_id="create_tmpdir")

    (t_log_info
     >> t_create_tmpdir
     >> t_build_dataset_lists
     >> t_emit_files_json
     )

