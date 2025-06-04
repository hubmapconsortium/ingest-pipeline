import json
import utils
from pathlib import Path
from datetime import datetime, timedelta
from pprint import pprint
from requests import codes
from requests.exceptions import HTTPError

import frontmatter

from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook

from hubmap_operators.common_operators import (
    CreateTmpDirOperator,
    CleanupTmpDirOperator,
    LogInfoOperator,
    JoinOperator,
    MoveDataOperator,
)

from utils import (
    localized_assert_json_matches_schema as assert_json_matches_schema,
    get_tmp_dir_path,
    get_auth_tok,
    get_dataset_uuid,
    get_previous_revision_uuid,
    build_dataset_name as inner_build_dataset_name,
    pythonop_get_dataset_state,
    pythonop_set_dataset_state,
    pythonop_send_create_dataset,
    pythonop_maybe_keep,
    get_parent_dataset_uuids_list,
    HMDAG,
    get_queue_resource,
    get_preserve_scratch_resource,
    get_uuid_for_error,
    create_dataset_state_error_callback,
    make_send_status_msg_function,
)

# Following are defaults which can be overridden later on
default_args = {
    "owner": "hubmap",
    "depends_on_past": False,
    "start_date": datetime(2023, 5, 1),
    "email": ["welling@psc.edu"],
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": create_dataset_state_error_callback(get_uuid_for_error),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "xcom_push": True,
    "queue": get_queue_resource("gen_pub_ancillary"),
}

with HMDAG(
    "gen_pub_ancillary",
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args=default_args,
    user_defined_macros={
        "tmp_dir_path": get_tmp_dir_path,
        "preserve_scratch": get_preserve_scratch_resource("gen_pub_ancillary"),
    },
) as dag:

    pipeline_name = "gen_pub_ancillary"

    def build_dataset_name(**kwargs):
        return inner_build_dataset_name(dag.dag_id, pipeline_name, **kwargs)

    def find_uuid(**kwargs):
        parent_l = kwargs["dag_run"].conf["parent_submission_id"]
        if isinstance(parent_l, list):
            assert len(parent_l) == 1, "Too many parents?"
            uuid = parent_l[0]
        else:
            uuid = parent_l

        def my_callable(**kwargs):
            return uuid

        ds_rslt = pythonop_get_dataset_state(dataset_uuid_callable=my_callable, **kwargs)
        if not ds_rslt:
            raise AirflowException(f"Invalid uuid/doi for group: {uuid}")
        print("ds_rslt:")
        pprint(ds_rslt)

        for key in ["entity_type", "status", "uuid", "dataset_type", "local_directory_full_path"]:
            assert key in ds_rslt, f"Dataset status for {uuid} has no {key}"

        if ds_rslt["entity_type"] != "Publication":
            raise AirflowException(f"{uuid} is not a Publication")
        if ds_rslt["status"] not in ["New", "Submitted", "QA"]:
            raise AirflowException(
                f"status of Upload {uuid} is not New, Submitted, Invalid, or Processing"
            )

        lz_path = ds_rslt["local_directory_full_path"]
        uuid = ds_rslt["uuid"]  # 'uuid' may  actually be a HuBMAP ID
        print(f"Finished uuid {uuid}")
        print(f"lz path: {lz_path}")
        kwargs["ti"].xcom_push(key="lz_path", value=lz_path)
        kwargs["ti"].xcom_push(key="uuid", value=uuid)

    t_find_uuid = PythonOperator(
        task_id="find_uuid",
        python_callable=find_uuid,
        provide_context=True,
    )

    def get_pub_description(uuid, **kwargs):
        method = "GET"
        auth_tok = get_auth_tok(**kwargs)
        headers = {
            "authorization": f"Bearer {auth_tok}",
            "content-type": "application/json",
            "X-Hubmap-Application": "ingest-pipeline",
        }
        http_hook = HttpHook(method, http_conn_id="entity_api_connection")
        endpoint = f"entities/{uuid}"
        try:
            response = http_hook.run(
                endpoint, headers=headers, extra_options={"check_response": False}
            )
            response.raise_for_status()
            ds_rslt = response.json()
            return ds_rslt["description"]
        except HTTPError as excp:
            print(f"ERROR: {excp}")
            if excp.response.status_code == codes.unauthorized:
                raise RuntimeError("entity database authorization was rejected?")
            else:
                print("benign error")
                return ""

    def build_ancillary_data(**kwargs):
        try:
            uuid = kwargs["ti"].xcom_pull(key="uuid")
            lz_path = kwargs["ti"].xcom_pull(key="lz_path")
            top_path = Path(lz_path)
            vignette_path = top_path / "vignettes"
            assert vignette_path.is_dir(), "vignettes not found"
            rslt = {"description": get_pub_description(uuid, **kwargs), "vignettes": []}

            for this_vignette_path in vignette_path.glob("*"):
                # print(f'this_vignette_path {this_vignette_path}')
                assert this_vignette_path.is_dir(), (
                    f"Found the non-dir {this_vignette_path}" " in vignettes"
                )
                vig_block = {"figures": [], "directory_name": str(this_vignette_path.name)}
                this_vignette_all_paths = set(this_vignette_path.glob("*"))
                # print('this_vignette_all_paths follows')
                # pprint(this_vignette_all_paths)
                if not all(pth.is_file() for pth in this_vignette_all_paths):
                    raise AssertionError("Found a subdirectory in a vignette")
                md_found = False
                for md_path in this_vignette_path.glob("*.md"):
                    # print(f'md in this vignette: {md_path}')
                    if md_found:
                        raise AssertionError("A vignette has more than one markdown file")
                    else:
                        md_found = True
                    vig_fm = frontmatter.loads(md_path.read_text())
                    # print('vig_fm metadata follows')
                    # pprint(vig_fm.metadata)
                    # print('vig_fm content follows')
                    # pprint(vig_fm.content)
                    for key in ["name", "figures"]:
                        assert key in vig_fm.metadata, f"vignette markdown has no {key}"
                    vig_block["name"] = vig_fm.metadata["name"]
                    vig_block["description"] = vig_fm.content
                    for fig_dict in vig_fm.metadata["figures"]:
                        assert "file" in fig_dict, "figure dict does not reference a file"
                        assert "name" in fig_dict, "figure dict does not provide a name"
                        vig_block["figures"].append(
                            {
                                "name": fig_dict["name"],
                                "file": fig_dict["file"],
                            }
                        )
                    this_vignette_all_paths.remove(md_path)
                    for fig in vig_block["figures"]:
                        this_vignette_all_paths.remove(this_vignette_path / fig["file"])
                assert not this_vignette_all_paths, (
                    "unexpected files in vignette:" f" {this_vignette_all_paths}"
                )

                rslt["vignettes"].append(vig_block)

            print("rslt follows")
            pprint(rslt)
            assert_json_matches_schema(rslt, "publication_ancillary_schema.yml")
            tmpdir = get_tmp_dir_path(kwargs["run_id"])
            tmpdir_path = Path(tmpdir)
            (tmpdir_path / "cwl_out").mkdir(exist_ok=True)
            with open(f"{tmpdir}/cwl_out/publication_ancillary.json", "w") as f:
                json.dump(rslt, f, indent=4)
            kwargs["ti"].xcom_push(key="return_value", value=0)  # signal success
        except AssertionError as excp:
            print(f"Operation failed because of: {excp}")
            kwargs["ti"].xcom_push(key="return_value", value=1)  # signal failure

    t_build_ancillary_data = PythonOperator(
        task_id="build_ancillary_data",
        python_callable=build_ancillary_data,
        provide_context=True,
    )

    send_status_msg = make_send_status_msg_function(
        dag_file=__file__,
        retcode_ops=["build_ancillary_data", "move_data"],
        cwl_workflows=[],
    )

    t_send_status = PythonOperator(
        task_id="send_status_msg",
        python_callable=send_status_msg,
        provide_context=True,
    )

    t_send_create_dataset = PythonOperator(
        task_id="send_create_dataset",
        python_callable=pythonop_send_create_dataset,
        provide_context=True,
        op_kwargs={
            "parent_dataset_uuid_callable": get_parent_dataset_uuids_list,
            "previous_revision_uuid_callable": get_previous_revision_uuid,
            "http_conn_id": "ingest_api_connection",
            "dataset_name_callable": build_dataset_name,
            "pipeline_shorthand": "ancillary",
        },
    )

    t_set_dataset_error = PythonOperator(
        task_id="set_dataset_error",
        python_callable=pythonop_set_dataset_state,
        provide_context=True,
        trigger_rule="all_done",
        op_kwargs={
            "dataset_uuid_callable": get_dataset_uuid,
            "ds_state": "Error",
            "message": "An error occurred in {}".format(pipeline_name),
        },
    )

    t_maybe_keep_ancillary_data = BranchPythonOperator(
        task_id="maybe_keep_ancillary_data",
        python_callable=pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "maybe_create_dataset",
            "bail_op": "set_dataset_error",
            "test_op": "build_ancillary_data",
        },
    )

    t_maybe_create_dataset = BranchPythonOperator(
        task_id="maybe_create_dataset",
        python_callable=utils.pythonop_dataset_dryrun,
        provide_context=True,
        op_kwargs={
            "next_op": "send_create_dataset",
            "bail_op": "join",
        },
    )

    t_log_info = LogInfoOperator(task_id="log_info")
    t_join = JoinOperator(task_id="join")
    t_move_data = MoveDataOperator(task_id="move_data")
    t_create_tmpdir = CreateTmpDirOperator(task_id="create_temp_dir")
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id="cleanup_temp_dir")

    # DAG
    (
        t_log_info
        >> t_create_tmpdir

        >> t_find_uuid
        >> t_build_ancillary_data
        >> t_maybe_keep_ancillary_data
        >> t_maybe_create_dataset

        >> t_send_create_dataset
        >> t_move_data
        >> t_send_status
        >> t_join
    )
    t_maybe_keep_ancillary_data >> t_set_dataset_error
    t_set_dataset_error >> t_join
    t_maybe_create_dataset >> t_join
    t_join >> t_cleanup_tmpdir
