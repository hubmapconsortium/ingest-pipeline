import utils
import os
from pathlib import Path
from pprint import pprint
from datetime import datetime, timedelta

from airflow.configuration import conf as airflow_conf
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook

from hubmap_operators.common_operators import (
    LogInfoOperator,
    JoinOperator,
    CreateTmpDirOperator,
    CleanupTmpDirOperator,
)

from utils import (
    pythonop_maybe_keep,
    pythonop_maybe_multiassay,
    get_tmp_dir_path,
    get_auth_tok,
    pythonop_get_dataset_state,
    pythonop_set_dataset_state,
    find_matching_endpoint,
    HMDAG,
    get_queue_resource,
    get_preserve_scratch_resource,
)

from misc.tools.split_and_create import reorganize


# Following are defaults which can be overridden later on
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
    "queue": get_queue_resource("reorganize_upload"),
}


def _get_frozen_df_path(run_id):
    # This version of the path is passed to the internals of
    # split_and_create, and must contain formatting space for
    # a suffix.
    return str(Path(get_tmp_dir_path(run_id)) / "frozen_source_df{}.tsv")


def _get_frozen_df_wildcard(run_id):
    # This version of the path is used from a bash command line
    # and must match all frozen_df files regardless of suffix.
    return str(Path(get_tmp_dir_path(run_id)) / "frozen_source_df*.tsv")


with HMDAG(
    "reorganize_upload",
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args=default_args,
    user_defined_macros={
        "tmp_dir_path": get_tmp_dir_path,
        "frozen_df_path": _get_frozen_df_path,
        "frozen_df_wildcard": _get_frozen_df_wildcard,
        "preserve_scratch": get_preserve_scratch_resource("reorganize_upload"),
    },
) as dag:

    def find_uuid(**kwargs):
        uuid = kwargs["dag_run"].conf["uuid"]

        def my_callable(**kwargs):
            return uuid

        ds_rslt = pythonop_get_dataset_state(dataset_uuid_callable=my_callable, **kwargs)
        if not ds_rslt:
            raise AirflowException(f"Invalid uuid/doi for group: {uuid}")
        print("ds_rslt:")
        pprint(ds_rslt)

        for key in ["entity_type", "status", "uuid", "dataset_type", "local_directory_full_path"]:
            assert key in ds_rslt, f"Dataset status for {uuid} has no {key}"

        if ds_rslt["entity_type"] != "Upload":
            raise AirflowException(f"{uuid} is not an Upload")
        if ds_rslt["status"] not in ["Valid", "Processing"]:
            raise AirflowException(
                f"status of Upload {uuid} is not Valid, or Processing, {ds_rslt['status']}"
            )

        lz_path = ds_rslt["local_directory_full_path"]
        uuid = ds_rslt["uuid"]  # 'uuid' may  actually be a DOI
        print(f"Finished uuid {uuid}")
        print(f"lz path: {lz_path}")
        kwargs["ti"].xcom_push(key="lz_path", value=lz_path)
        kwargs["ti"].xcom_push(key="uuid", value=uuid)

    t_find_uuid = PythonOperator(
        task_id="find_uuid", python_callable=find_uuid, provide_context=True, op_kwargs={}
    )

    t_create_tmpdir = CreateTmpDirOperator(task_id="create_tmpdir")
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id="cleanup_tmpdir")

    t_preserve_info = BashOperator(
        task_id="preserve_info",
        bash_command="""
        frozen_df_wildcard="{{frozen_df_wildcard(run_id)}}" ; \
        upload_path="{{ti.xcom_pull(task_ids="find_uuid", key="lz_path")}}" ; \
        if ls $frozen_df_wildcard > /dev/null 2>&1 ; then \
          cp ${frozen_df_wildcard} "${upload_path}" ; \
        fi
        """,
    )

    def split_stage_1(**kwargs):
        uuid = kwargs["ti"].xcom_pull(task_ids="find_uuid", key="uuid")
        entity_host = HttpHook.get_connection("entity_api_connection").host
        try:
            reorganize(
                uuid,
                mode="stop",
                ingest=False,
                # dryrun=True,
                dryrun=False,
                instance=find_matching_endpoint(entity_host),
                auth_tok=get_auth_tok(**kwargs),
                frozen_df_fname=_get_frozen_df_path(kwargs["run_id"]),
            )
            kwargs["ti"].xcom_push(key="split_stage_1", value="0")  # signal success
        except Exception as e:
            print(f"Encountered {e}")
            kwargs["ti"].xcom_push(key="split_stage_1", value="1")  # signal failure

    t_split_stage_1 = PythonOperator(
        task_id="split_stage_1", python_callable=split_stage_1, provide_context=True, op_kwargs={}
    )

    t_maybe_keep_1 = BranchPythonOperator(
        task_id="maybe_keep_1",
        python_callable=pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "split_stage_2",
            "bail_op": "set_dataset_error",
            "test_op": "split_stage_1",
        },
    )

    def split_stage_2(**kwargs):
        work_dirs = []
        uuid = kwargs["ti"].xcom_pull(task_ids="find_uuid", key="uuid")
        entity_host = HttpHook.get_connection("entity_api_connection").host
        try:
            child_uuid_list, is_multiassay = reorganize(
                uuid,
                mode="unstop",
                ingest=False,
                # dryrun=True,
                dryrun=False,
                instance=find_matching_endpoint(entity_host),
                auth_tok=get_auth_tok(**kwargs),
                frozen_df_fname=_get_frozen_df_path(kwargs["run_id"]),
            )
            kwargs["ti"].xcom_push(key="split_stage_2", value="0")  # signal success
            kwargs["ti"].xcom_push(key="is_multiassay", value=is_multiassay)
            for uuid in child_uuid_list:

                def my_callable(**kwargs):
                    return uuid

                ds_rslt = pythonop_get_dataset_state(dataset_uuid_callable=my_callable, **kwargs)
                if not ds_rslt:
                    raise AirflowException(f"Invalid uuid/doi for group: {uuid}")
                print("ds_rslt:")
                pprint(ds_rslt)

                for key in [
                    "entity_type",
                    "status",
                    "uuid",
                    "dataset_type",
                    "local_directory_full_path",
                ]:
                    assert key in ds_rslt, f"Dataset status for {uuid} has no {key}"

                if ds_rslt["entity_type"] != "Dataset":
                    raise AirflowException(f"{uuid} is not a Dataset")
                if ds_rslt["status"] not in ["New", "Submitted"]:
                    raise AirflowException(
                        f"status of Upload {uuid} is not New, or Submitted, {ds_rslt['status']}"
                    )

                work_dirs.append(ds_rslt["local_directory_full_path"])
            work_dirs = " ".join(work_dirs)
            kwargs["ti"].xcom_push(key="child_work_dirs", value=work_dirs)
            kwargs["ti"].xcom_push(key="child_uuid_list", value=child_uuid_list)
        except Exception as e:
            print(f"Encountered {e}")
            kwargs["ti"].xcom_push(key="split_stage_2", value="1")  # signal failure

    t_split_stage_2 = PythonOperator(
        task_id="split_stage_2", python_callable=split_stage_2, provide_context=True, op_kwargs={}
    )

    t_maybe_keep_2 = BranchPythonOperator(
        task_id="maybe_keep_2",
        python_callable=pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "run_md_extract",
            "bail_op": "set_dataset_error",
            "test_op": "split_stage_2",
        },
    )

    t_run_md_extract = BashOperator(
        task_id="run_md_extract",
        bash_command=""" \
            src_dir="{{dag_run.conf.src_path}}/md" ; \
            top_dir="{{dag_run.conf.src_path}}" ; \
            work_dir="{{tmp_dir_path(run_id)}}" ; \
            cd $work_dir ; \
            env PYTHONPATH=${PYTHONPATH}:$top_dir \
            for lz_dir in ${WORK_DIRS[@]}; \
            do;
            ${PYTHON_EXE} $src_dir/metadata_extract.py --out ./${lz_dir##*/}-rslt.yml --yaml "$lz_dir" \
              >> session.log 2> error.log ;\
            echo $? ; \
            if [ -s error.log ] ; \
            then echo 'ERROR!' `cat error.log` >> session.log ; \
            else rm error.log ; \
            fi; \
            done;
            """,
        env={
            "AUTH_TOK": (
                utils.get_auth_tok(
                    **{
                        "crypt_auth_tok": utils.encrypt_tok(
                            airflow_conf.as_dict()["connections"]["APP_CLIENT_SECRET"]
                        ).decode()
                    }
                )
            ),
            "PYTHON_EXE": os.environ["CONDA_PREFIX"] + "/bin/python",
            "INGEST_API_URL": os.environ["AIRFLOW_CONN_INGEST_API_CONNECTION"],
            "WORK_DIRS": t_split_stage_2.xcom_pull("child_work_dirs"),
        },
    )

    t_md_consistency_tests = PythonOperator(
        task_id="md_consistency_tests",
        python_callable=utils.pythonop_md_consistency_tests,
        provide_context=True,
        op_kwargs={
            "metadata_fname": "rslt.yml",
            "uuid_list": t_split_stage_2.xcom_pull("child_uuid_list"),
        },
    )

    t_log_info = LogInfoOperator(task_id="log_info")

    t_join = JoinOperator(task_id="join")

    def _get_upload_uuid(**kwargs):
        return kwargs["ti"].xcom_pull(task_ids="find_uuid", key="uuid")

    t_set_dataset_error = PythonOperator(
        task_id="set_dataset_error",
        python_callable=pythonop_set_dataset_state,
        provide_context=True,
        trigger_rule="all_done",
        op_kwargs={"dataset_uuid_callable": _get_upload_uuid, "ds_state": "Error"},
    )

    (
        t_log_info
        >> t_find_uuid
        >> t_create_tmpdir
        >> t_split_stage_1
        >> t_maybe_keep_1
        >> t_split_stage_2
        >> t_maybe_keep_2
        >> t_run_md_extract
        >> t_md_consistency_tests
        # ToDo:
        #  Send extracted metadata.
        >> t_join
        >> t_preserve_info
        >> t_cleanup_tmpdir
        # ToDo:
        #  multiassay downstream for create dataset components and build metadata.
    )

    t_maybe_keep_1 >> t_set_dataset_error
    t_maybe_keep_2 >> t_set_dataset_error
    t_set_dataset_error >> t_join
