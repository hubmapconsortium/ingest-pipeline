import urllib.parse

from datetime import datetime, timedelta
from pathlib import Path

from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.operators.python import BranchPythonOperator, PythonOperator

import utils
from utils import (
    downstream_workflow_iter,
    get_dataset_uuid,
    get_absolute_workflow,
    get_parent_data_dir,
    build_dataset_name as inner_build_dataset_name,
    get_uuid_for_error,
    join_quote_command_str,
    get_tmp_dir_path,
    HMDAG,
    get_queue_resource,
    get_preserve_scratch_resource,
    get_cwl_cmd_from_workflows,
    get_submission_context,
    get_auth_tok,
    get_parent_dataset_uuid,
    post_to_slack_notify,
    env_appropriate_slack_channel
)

from hubmap_operators.common_operators import (
    CreateTmpDirOperator,
    LogInfoOperator,
)
from hubmap_operators.flex_multi_dag_run import FlexMultiDagRunOperator

from airflow.configuration import conf as airflow_conf

from extra_utils import build_tag_containers

SLACK_NOTIFY_CHANNEL = "C07P2P1D5LP"

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
    "queue": get_queue_resource("celldive_deepcell"),
    "on_failure_callback": utils.create_dataset_state_error_callback(get_uuid_for_error),
}

with HMDAG(
    "celldive_deepcell_segmentation",
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args=default_args,
    user_defined_macros={
        "tmp_dir_path": get_tmp_dir_path,
        "preserve_scratch": get_preserve_scratch_resource("celldive_deepcell"),
    },
) as dag:
    pipeline_name = "celldive-pipeline"
    workflow_version = "1.0.0"
    workflow_description = "The CellDive pipeline performs segments nuclei and cells using Cytokit, and performs spatial analysis of expression data using SPRM, which computes various measures of analyte intensity per cell, performs clustering based on expression and other data, and computes markers for each cluster."

    cwl_workflows = [
        {
            "workflow_path": str(
                get_absolute_workflow(Path("phenocycler-pipeline", "pipeline.cwl"))
            ),
            "documentation_url": "",
        },
        {
            "workflow_path": str(
                get_absolute_workflow(Path("stellar-outofband", "steps", "pre-convert.cwl"))
            ),
            "documentation_url": "",
        },
        {
            "workflow_path": str(get_absolute_workflow(Path("sprm", "pipeline.cwl"))),
            "documentation_url": "",
        },
        {
            "workflow_path": str(
                get_absolute_workflow(Path("create-vis-symlink-archive", "pipeline.cwl"))
            ),
            "documentation_url": "",
        },
        {
            "workflow_path": str(get_absolute_workflow(Path("ome-tiff-pyramid", "pipeline.cwl"))),
            "documentation_url": "",
        },
        {
            "workflow_path": str(
                get_absolute_workflow(Path("portal-containers", "ome-tiff-offsets.cwl"))
            ),
            "documentation_url": "",
        },
        {
            "workflow_path": str(
                get_absolute_workflow(Path("portal-containers", "sprm-to-json.cwl"))
            ),
            "documentation_url": "",
        },
        {
            "workflow_path": str(
                get_absolute_workflow(Path("portal-containers", "sprm-to-anndata.cwl"))
            ),
            "documentation_url": "",
        },
    ]

    def build_dataset_name(**kwargs):
        return inner_build_dataset_name(dag.dag_id, pipeline_name, **kwargs)

    @task(task_id="prepare_cwl_segmentation")
    def prepare_cwl_cmd1(**kwargs):
        if kwargs["dag_run"].conf.get("dryrun"):
            cwl_path = Path(cwl_workflows[0]["workflow_path"]).parent
            return build_tag_containers(cwl_path)
        else:
            return "No Container build required"

    prepare_cwl_segmentation = prepare_cwl_cmd1()

    def build_cwltool_cwl_segmentation(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("mpdir: ", tmpdir)
        data_dir = get_parent_data_dir(**kwargs)
        print("data_dir: ", data_dir)

        workflow = cwl_workflows[0]
        meta_yml_path = str(Path(workflow["workflow_path"]).parent / "meta.yaml")

        input_parameters = [
            {"parameter_name": "--gpus", "value": "all"},
            {"parameter_name": "--segmentation_method", "value": "deepcell"},
            {"parameter_name": "--data_dir", "value": str(data_dir)},
            {"parameter_name": "--invert_geojson_mask", "value": ""},
        ]

        command = get_cwl_cmd_from_workflows(
            cwl_workflows, 0, input_parameters, tmpdir, kwargs["ti"]
        )

        return join_quote_command_str(command)

    t_build_cwl_segmentation = PythonOperator(
        task_id="build_cwl_segmentation",
        python_callable=build_cwltool_cwl_segmentation,
        provide_context=True,
    )

    t_pipeline_exec_cwl_segmentation = BashOperator(
        task_id="pipeline_exec_cwl_segmentation",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        mkdir -p ${tmp_dir}/cwl_out ; \
        {{ti.xcom_pull(task_ids='build_cwl_segmentation')}} > $tmp_dir/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_maybe_keep_cwl_segmentation = BranchPythonOperator(
        task_id="maybe_keep_cwl_segmentation",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_stellar_pre_convert",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_segmentation",
        },
    )


    @task(task_id="prepare_stellar_pre_convert")
    def prepare_cwl_stellar_pre_convert(**kwargs):
        if kwargs["dag_run"].conf.get("dryrun"):
            cwl_path = Path(cwl_workflows[1]["workflow_path"]).parent
            return build_tag_containers(cwl_path)
        else:
            return "No Container build required"

    prepare_stellar_pre_convert = prepare_cwl_stellar_pre_convert()

    def build_cwltool_cwl_stellar_pre_convert(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)

        workflows = kwargs["ti"].xcom_pull(key="cwl_workflows", task_ids="build_cwl_segmentation")

        input_parameters = [
            {"parameter_name": "--directory", "value": str(tmpdir / "cwl_out")},
        ]
        command = get_cwl_cmd_from_workflows(workflows, 1, input_parameters, tmpdir, kwargs["ti"])

        return join_quote_command_str(command)

    t_build_cwl_stellar_pre_convert = PythonOperator(
        task_id="build_cwl_stellar_pre_convert",
        python_callable=build_cwltool_cwl_stellar_pre_convert,
        provide_context=True,
    )

    t_pipeline_exec_cwl_stellar_pre_convert = BashOperator(
        task_id="pipeline_exec_cwl_stellar_pre_convert",
        bash_command=""" \
            tmp_dir={{tmp_dir_path(run_id)}} ; \
            {{ti.xcom_pull(task_ids='build_cwl_stellar_pre_convert')}} >> $tmp_dir/session.log 2>&1 ; \
            echo $?
            """,
    )

    t_maybe_keep_cwl_stellar_pre_convert = BranchPythonOperator(
        task_id="maybe_keep_cwl_stellar_pre_convert",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "t_copy_stellar_pre_convert_data",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_stellar_pre_convert",
        },
    )

    #  output is a single file cell_data.h5ad
    #  copy this output to some hardcoded directory
    t_copy_stellar_pre_convert_data = BashOperator(
        task_id="copy_stellar_pre_convert_data",
        bash_command=""" \
            mkdir /hive/hubmap/data/projects/STELLAR_pre_convert/{{ dag_run.conf.parent_submission_id | replace("[", "") | replace("]", "") }}/
            tmp_dir={{tmp_dir_path(run_id)}} ; \
            find ${tmp_dir} -name "cell_data.h5ad" -exec cp -v {} /hive/hubmap/data/projects/STELLAR_pre_convert/{{ dag_run.conf.parent_submission_id | replace("[", "") | replace("]", "") }} \; ; \
            echo $?
            """,
    )

    @task
    def notify_user_stellar_pre_convert(**kwargs):
        run_id = kwargs["run_id"]
        conf = airflow_conf.as_dict().get("webserver", {})
        # ensure base_url uses https
        base_url = (
            urllib.parse.urlparse(str(conf.get("base_url", "")))._replace(scheme="https").geturl()
        )
        run_url = f"{base_url}:{conf.get('web_server_port', '')}/dags/phenocycler_deepcell_segmentation/grid?dag_run_id={urllib.parse.quote(run_id)}"
        primary_id = get_submission_context(
            get_auth_tok(**kwargs), get_parent_dataset_uuid(**kwargs)
        ).get("hubmap_id")
        message = f"CellDIVE segmentation step succeeded in run <{run_url}|{run_id}>. Primary dataset ID: {primary_id}."
        if kwargs["dag_run"].conf.get("dryrun"):
            message = "[dryrun] " + message
        post_to_slack_notify(
            get_auth_tok(**kwargs), message, env_appropriate_slack_channel(SLACK_NOTIFY_CHANNEL)
        )

    t_notify_user_stellar_pre_convert = notify_user_stellar_pre_convert()

    t_set_dataset_error = PythonOperator(
        task_id="set_dataset_error",
        python_callable=utils.pythonop_set_dataset_state,
        provide_context=True,
        trigger_rule="all_done",
        op_kwargs={
            "dataset_uuid_callable": get_dataset_uuid,
            "ds_state": "Error",
            "message": "An error occurred in {}".format(pipeline_name),
            "pipeline_name": pipeline_name
        },
    )

    t_log_info = LogInfoOperator(task_id="log_info")
    t_create_tmpdir = CreateTmpDirOperator(task_id="create_tmpdir")

    def trigger_sprm(**kwargs):
        collection_type = kwargs.get("collection_type", "")
        assay_type = kwargs.get("assay_type", "")
        payload = {
            "tmp_dir": get_tmp_dir_path(kwargs["run_id"]),
            "parent_submission_id": kwargs["dag_run"].conf.get("parent_submission_id"),
            "parent_lz_path": kwargs["dag_run"].conf.get("parent_lz_path"),
            "previous_version_uuid": kwargs["dag_run"].conf.get("previous_version_uuid"),
            "metadata": kwargs["dag_run"].conf.get("metadata"),
            "crypt_auth_tok": kwargs["dag_run"].conf.get("crypt_auth_tok"),
            "workflows": kwargs["ti"].xcom_pull(
                task_ids="build_cwl_segmentation", key="cwl_workflows"
            ),
        }
        print(
            f"Collection_type: {collection_type} with assay_type {assay_type} and payload: {payload}",
        )
        for next_dag in downstream_workflow_iter(collection_type, assay_type):
            yield next_dag, payload

    t_trigger_phenocyler = FlexMultiDagRunOperator(
        task_id="trigger_sprm",
        dag=dag,
        trigger_dag_id="celldive_segmentation",
        python_callable=trigger_sprm,
        op_kwargs={"collection_type": "celldive_sprm", "assay_type": "celldive"},
    )

    (
        t_log_info
        >> t_create_tmpdir

        >> prepare_cwl_segmentation
        >> t_build_cwl_segmentation
        >> t_pipeline_exec_cwl_segmentation
        >> t_maybe_keep_cwl_segmentation

        >> prepare_stellar_pre_convert
        >> t_build_cwl_stellar_pre_convert
        >> t_pipeline_exec_cwl_stellar_pre_convert
        >> t_maybe_keep_cwl_stellar_pre_convert
        >> t_copy_stellar_pre_convert_data
        >> t_notify_user_stellar_pre_convert
        >> t_trigger_phenocyler

    )
    t_maybe_keep_cwl_segmentation >> t_set_dataset_error
    t_maybe_keep_cwl_stellar_pre_convert >> t_set_dataset_error
