from pathlib import Path
from pprint import pprint
from datetime import datetime, timedelta
import time
import os
import yaml

from airflow.configuration import conf as airflow_conf
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
    get_tmp_dir_path,
    get_auth_tok,
    pythonop_get_dataset_state,
    pythonop_set_dataset_state,
    find_matching_endpoint,
    HMDAG,
    get_queue_resource,
    get_preserve_scratch_resource,
    encrypt_tok,
    make_send_status_msg_function,
    search_api_reindex,
)

from extra_utils import get_components

from misc.tools.split_and_create import reorganize_multiassay


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


def get_dataset_lz_path(**kwargs) -> str:
    uuid = kwargs["uuid_dataset"]

    def my_callable(**kwargs):
        return uuid

    ds_rslt = pythonop_get_dataset_state(dataset_uuid_callable=my_callable, **kwargs)
    if not ds_rslt:
        raise AirflowException(f"Invalid uuid/doi for group: {uuid}")
    print("ds_rslt:")
    pprint(ds_rslt)

    return ds_rslt["local_directory_full_path"]


def get_dataset_uuid(**kwargs):
    return kwargs["uuid_dataset"]


def read_metadata_file(**kwargs):
    md_fname = os.path.join(
        get_tmp_dir_path(kwargs["run_id"]),
        f"{kwargs['uuid_parent_dataset']}-{kwargs['uuid_dataset_type']}-rslt.yml",
    )
    with open(md_fname, "r") as f:
        scanned_md = yaml.safe_load(f)
    return scanned_md


with HMDAG(
    "reorganize_multiassay",
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args=default_args,
    user_defined_macros={
        "tmp_dir_path": get_tmp_dir_path,
        "frozen_df_path": _get_frozen_df_path,
        "frozen_df_wildcard": _get_frozen_df_wildcard,
        "preserve_scratch": get_preserve_scratch_resource("reorganize_multiassay"),
    },
) as dag:

    # This section verifies the parent datasets
    def check_one_uuid(uuid, **kwargs):
        """
        Look up information on the given uuid or HuBMAP identifier.
        Returns:
        - the uuid, translated from an identifier if necessary
        - data type(s) of the dataset
        - local directory full path of the dataset
        """
        print(f"Starting uuid {uuid}")
        my_callable = lambda **kwargs: uuid
        ds_rslt = pythonop_get_dataset_state(dataset_uuid_callable=my_callable, **kwargs)
        if not ds_rslt:
            raise AirflowException(f"Invalid uuid/doi for group: {uuid}")
        print("ds_rslt:")
        pprint(ds_rslt)

        for key in ["status", "uuid", "local_directory_full_path", "metadata", "dataset_type"]:
            assert key in ds_rslt, f"Dataset status for {uuid} has no {key}"

        if ds_rslt["status"] not in ["New", "Submitted", "Error"]:
            raise AirflowException(f"Dataset {uuid} is not QA or better")

        return (
            ds_rslt["uuid"],
            ds_rslt["local_directory_full_path"],
            ds_rslt["metadata"],
            ds_rslt["dataset_type"],
        )

    def find_uuid(**kwargs):
        uuids = kwargs["dag_run"].conf["uuids"]

        ds_uuids = []
        lz_paths = []

        for uuid in uuids:
            ds_uuid, lz_path, _, _ = check_one_uuid(uuid, **kwargs)
            ds_uuids.append(ds_uuid)
            lz_paths.append(lz_path)

        kwargs["ti"].xcom_push(key="lz_paths", value=lz_paths)
        kwargs["ti"].xcom_push(key="uuids", value=ds_uuids)

        work_dirs = " ".join([f'"{lz_path}"' for lz_path in lz_paths])
        kwargs["ti"].xcom_push(key="work_dirs_for_md", value=work_dirs)

    t_find_uuid = PythonOperator(
        task_id="find_uuid", python_callable=find_uuid, provide_context=True, op_kwargs={}
    )

    t_create_tmpdir = CreateTmpDirOperator(task_id="create_tmpdir")
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id="cleanup_tmpdir")

    # # This section performs the component split
    # def split(**kwargs):
    #     ds_uuids = kwargs["ti"].xcom_pull(task_ids="find_uuid", key="uuids")
    #     entity_host = HttpHook.get_connection("entity_api_connection").host
    #
    #     for ds_uuid in ds_uuids:
    #         try:
    #             reorganize_multiassay(
    #                 ds_uuid,
    #                 # dryrun=True,
    #                 reindex=False,
    #                 dryrun=False,
    #                 instance=find_matching_endpoint(entity_host),
    #                 auth_tok=get_auth_tok(**kwargs),
    #             )
    #         except Exception as e:
    #             print(f"Encountered {e}")
    #             kwargs["ti"].xcom_push(key="split", value="1")  # signal failure
    #             return
    #         time.sleep(30)
    #     kwargs["ti"].xcom_push(key="split", value="0")  # signal success
    #
    # t_split = PythonOperator(
    #     task_id="split", python_callable=split, provide_context=True, op_kwargs={}
    # )
    #
    # t_maybe_keep = BranchPythonOperator(
    #     task_id="maybe_keep",
    #     python_callable=pythonop_maybe_keep,
    #     provide_context=True,
    #     op_kwargs={
    #         "next_op": "get_component_datasets",
    #         "bail_op": "set_dataset_error",
    #         "test_op": "split",
    #     },
    # )

    # Now we get all of the components
    def get_component_datasets(**kwargs):
        uuids = kwargs["ti"].xcom_pull(task_ids="find_uuid", key="uuids")
        all_components = {}
        dataset_types = []
        for uuid in uuids:
            components = get_components(uuid, get_auth_tok(**kwargs))
            dataset_types.extend([f'"{component["dataset_type"]}"' for component in components])
            all_components[uuid] = components

        dataset_types = list(set(dataset_types))
        dataset_types = " ".join(dataset_types)
        kwargs["ti"].xcom_push(key="ds_types_for_md", value=dataset_types)

        return all_components

    t_get_component_datasets = PythonOperator(
        task_id="get_component_datasets",
        python_callable=get_component_datasets,
        queue=get_queue_resource("rebuild_metadata"),
        provide_context=True,
    )

    t_log_info = LogInfoOperator(task_id="log_info")

    t_join = JoinOperator(task_id="join")

    # Extract the metadata for the components
    t_run_md_extract = BashOperator(
        task_id="run_md_extract",
        bash_command=""" \
                set -x ; \
                src_dir="{{dag_run.conf.src_path}}/md" ; \
                top_dir="{{dag_run.conf.src_path}}" ; \
                work_dir="{{tmp_dir_path(run_id)}}" ; \
                component_types=({{ti.xcom_pull(task_ids='get_component_datasets', key='ds_types_for_md')}}) ; \
                cd $work_dir ; \
                export WORK_DIRS=({{ti.xcom_pull(task_ids='find_uuid', key='work_dirs_for_md')}}); \
                for lz_dir in "${WORK_DIRS[@]}"; \
                do \
                # NEED TO LOOP OVER COMPONENTS
                for component_type in "${component_types[@]}"; \
                do \
                env PYTHONPATH=${PYTHONPATH}:$top_dir \
                ${PYTHON_EXE} $src_dir/metadata_extract.py --out ./${lz_dir##*/}-$component_type-rslt.yml --yaml \
                "$lz_dir" --component $component_type >> session.log 2> error.log ;\
                done; \
                done; \
                if [ -s error.log ] ; \
                then \
                echo 'ERROR!' `cat error.log` >> session.log ; \
                echo 1; \
                else \
                rm error.log ; \
                echo 0; \
                fi; \
                """,
        env={
            "AUTH_TOK": (
                get_auth_tok(
                    **{
                        "crypt_auth_tok": encrypt_tok(
                            airflow_conf.as_dict()["connections"]["APP_CLIENT_SECRET"]
                        ).decode()
                    }
                )
            ),
            "PYTHON_EXE": os.environ["CONDA_PREFIX"] + "/bin/python",
            "INGEST_API_URL": os.environ["AIRFLOW_CONN_INGEST_API_CONNECTION"],
            "COMPONENTS_ASSAY_TYPE": "1",
        },
    )

    # TODO: Do we need this
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

    send_status_msg = make_send_status_msg_function(
        dag_file=__file__,
        retcode_ops=["run_md_extract", "md_consistency_tests"],
        cwl_workflows=[],
        dataset_uuid_fun=get_dataset_uuid,
        dataset_lz_path_fun=get_dataset_lz_path,
        metadata_fun=read_metadata_file,
        include_file_metadata=False,
        reindex=False,
    )

    def wrapped_send_status_msg(**kwargs):
        components = kwargs["ti"].xcom_pull(task_ids="get_component_datasets")
        uuids = kwargs["dag_run"].conf["uuids"]
        for uuid in uuids:
            for component in components[uuid]:
                kwargs["uuid_parent_dataset"] = uuid
                kwargs["uuid_dataset_type"] = component["dataset_type"]
                kwargs["uuid_dataset"] = component["uuid"]
                if send_status_msg(**kwargs):
                    scanned_md = read_metadata_file(**kwargs)  # Yes, it's getting re-read
                    print(
                        f"""Got CollectionType {scanned_md['collectiontype'] 
                        if 'collectiontype' in scanned_md else None} """
                    )
                else:
                    print(f"Something went wrong!!")
                    return 1
            time.sleep(30)
        return 0

    t_send_status = PythonOperator(
        task_id="send_status_msg",
        python_callable=wrapped_send_status_msg,
        provide_context=True,
        trigger_rule="all_done",
    )

    def reindex_routine(**kwargs):
        # TODO: Seems like we can just issue a re-index for the donors. But let's do it like this for now.
        parent_dataset_uuids = kwargs["ti"].xcom_pull(task_ids="find_uuid", key="uuids")
        component_uuids = [
            component["uuid"]
            for component in kwargs["ti"].xcom_pull(task_ids="get_component_datasets")
        ]

        for uuid in parent_dataset_uuids + component_uuids:
            if not search_api_reindex(uuid, **kwargs):
                return 1

            time.sleep(240)
        return 0

    t_reindex_routine = PythonOperator(
        task_id="reindex_routine",
        python_callable=reindex_routine,
        provide_context=True,
    )

    def _get_upload_uuid(**kwargs):
        return kwargs["ti"].xcom_pull(task_ids="find_uuid", key="uuid")

    t_set_dataset_error = PythonOperator(
        task_id="set_dataset_error",
        python_callable=pythonop_set_dataset_state,
        provide_context=True,
        trigger_rule="all_done",
        op_kwargs={
            "dataset_uuid_callable": _get_upload_uuid,
            "ds_state": "Error",
            "reindex": False,
        },
    )

    (
        t_log_info
        >> t_find_uuid
        >> t_create_tmpdir
        # >> t_split
        # >> t_maybe_keep
        >> t_get_component_datasets
        >> t_run_md_extract
        >> t_send_status
        >> t_reindex_routine
        >> t_join
        >> t_preserve_info
        >> t_cleanup_tmpdir
    )

    # t_maybe_keep >> t_set_dataset_error
    # t_set_dataset_error >> t_join
