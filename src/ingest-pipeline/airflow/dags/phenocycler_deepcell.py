import re
import urllib.parse
from datetime import datetime, timedelta
from pathlib import Path

from extra_utils import build_tag_containers
from hubmap_operators.common_operators import (
    CreateTmpDirOperator,
    LogInfoOperator,
)
from hubmap_operators.flex_multi_dag_run import FlexMultiDagRunOperator
from status_change.callbacks.failure_callback import FailureCallback
from utils import (
    HMDAG,
)
from utils import build_dataset_name as inner_build_dataset_name
from utils import (
    downstream_workflow_iter,
    get_absolute_workflow,
    get_auth_tok,
    get_cwl_cmd_from_workflows,
    get_dataset_uuid,
    get_parent_data_dir,
    get_preserve_scratch_resource,
    get_queue_resource,
    get_tmp_dir_path,
    get_uuid_for_error,
    join_quote_command_str,
    post_to_slack_notify,
    pythonop_maybe_keep,
    pythonop_set_dataset_state,
)

from airflow.configuration import conf as airflow_conf
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

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
    "queue": get_queue_resource("phenocycler_deepcell"),
    "on_failure_callback": FailureCallback(__name__, get_uuid_for_error),
}

with HMDAG(
    "phenocycler_deepcell_segmentation",
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args=default_args,
    user_defined_macros={
        "tmp_dir_path": get_tmp_dir_path,
        "preserve_scratch": get_preserve_scratch_resource("phenocycler_deepcell"),
    },
) as dag:
    pipeline_name = "phenocycler-pipeline"
    workflow_version = "1.0.0"
    workflow_description = "The Phenocycler pipeline begins with optional masking of user-specified regions defined in an optional geoJSON file describing areas of inclusino or exclusion.  It then breaks the image up into slices that can be reasonably segmented, runs cell and nuclear segmentation on those slices using DeepCell with the specified marker channels and stitches the segmentation masks back into one mask which can overlay the full expression image.  The segmentation masks and expression images are then used for cell type assignment using RIBCA, Stellar, and DeepCellTypes.  Finally, SPRM is used for spatial analysis of expression data, including computation of various measures of analyte intensity per cell, clustering based on expression and other data, marker computation per cluster, subclustering of cell types assigned by multiple methods and more."

    cwl_workflows = [
        {
            "workflow_path": str(get_absolute_workflow(Path(pipeline_name, "pipeline.cwl"))),
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
        print("tmpdir: ", tmpdir)
        data_dir = get_parent_data_dir(**kwargs)
        print("data_dir: ", data_dir)

        input_parameters = [
            {"parameter_name": "--gpus", "value": "all"},
            {"parameter_name": "--segmentation_method", "value": "deepcell"},
            {"parameter_name": "--data_dir", "value": str(data_dir)},
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
        python_callable=pythonop_maybe_keep,
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

    #  output is a single file cell_data.h5ad
    #  copy this output to some hardcoded directory
    t_copy_stellar_pre_convert_data = BashOperator(
        task_id="copy_stellar_pre_convert_data",
        bash_command=""" \
        mkdir /hive/hubmap/data/projects/STELLAR_pre_convert/{{ dag_run.conf.parent_submission_id }}/
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        find ${tmp_dir} -name "cell_data.h5ad" -exec cp -v {} /hive/hubmap/data/projects/STELLAR_pre_convert/{{ dag_run.conf.parent_submission_id }} \; ; \
        echo $?
        """,
    )

    t_maybe_keep_cwl_stellar_pre_convert = BranchPythonOperator(
        task_id="maybe_keep_cwl_stellar_pre_convert",
        python_callable=pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "notify_user_stellar_pre_convert",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_stellar_pre_convert",
        },
    )

    @task
    def notify_user_stellar_pre_convert(**kwargs):
        run_id = kwargs["run_id"]
        conf = airflow_conf.as_dict().get("webserver", {})
        run_url = f"{conf.get('base_url', '')}:{conf.get('web_server_port', '')}/dags/phenocycler_deepcell_segmentation/grid?dag_run_id={urllib.parse.quote(run_id)}"
        message = f"STELLAR pre-convert step succeeded in run <{run_url}|{run_id}>."
        if kwargs["dag_run"].conf.get("dryrun"):
            message = "[dryrun] " + message
        post_to_slack_notify(get_auth_tok(**kwargs), message, SLACK_NOTIFY_CHANNEL)

    t_notify_user_stellar_pre_convert = notify_user_stellar_pre_convert()

    @task(task_id="cell_count_cmd")
    def build_cell_count_cmd(**kwargs):
        tmpdir = get_tmp_dir_path(kwargs["run_id"])
        print("tmpdir: ", tmpdir)
        pattern = r"\: (\d+),$"
        num_cells_re = None
        with open(Path(tmpdir, "session.log"), "r") as f:
            for line in f:
                if "num_cells" in line:
                    num_cells_re = re.search(pattern, line)
        if not num_cells_re:
            raise Exception("'num_cells' not found in session.log file")
        num_cells = num_cells_re.group(1)
        print("num_cells: ", num_cells)
        kwargs["ti"].xcom_push(key="small_sprm", value=1 if int(num_cells) > 200000 else 0)
        return 0

    cell_count_cmd = build_cell_count_cmd()

    t_maybe_start_small_sprm = BranchPythonOperator(
        task_id="maybe_start_small",
        python_callable=pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "trigger_phenocycler_small",
            "bail_op": "trigger_phenocycler",
            "test_op": "cell_count_cmd",
            "test_key": "small_sprm",
        },
    )

    def trigger_phenocycler(**kwargs):
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
                task_ids="build_cwl_stellar_pre_convert", key="cwl_workflows"
            ),
        }
        print(
            f"Collection_type: {collection_type} with assay_type {assay_type} and payload: {payload}",
        )
        for next_dag in downstream_workflow_iter(collection_type, assay_type):
            yield next_dag, payload

    t_trigger_phenocyler_small = FlexMultiDagRunOperator(
        task_id="trigger_phenocycler_small",
        dag=dag,
        trigger_dag_id="phenocycler_segmentation",
        python_callable=trigger_phenocycler,
        op_kwargs={"collection_type": "small_phenocycler", "assay_type": "phenocycler"},
    )

    t_trigger_phenocyler = FlexMultiDagRunOperator(
        task_id="trigger_phenocycler",
        dag=dag,
        trigger_dag_id="phenocycler_segmentation",
        python_callable=trigger_phenocycler,
        op_kwargs={"collection_type": "phenocycler", "assay_type": "phenocycler"},
    )

    t_log_info = LogInfoOperator(task_id="log_info")
    t_create_tmpdir = CreateTmpDirOperator(task_id="create_tmpdir")

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
        >> t_copy_stellar_pre_convert_data
        >> t_maybe_keep_cwl_stellar_pre_convert
        >> t_notify_user_stellar_pre_convert
        >> cell_count_cmd
        >> t_maybe_start_small_sprm
        >> t_trigger_phenocyler_small
    )
    t_maybe_start_small_sprm >> t_trigger_phenocyler
    t_maybe_keep_cwl_segmentation >> t_set_dataset_error
