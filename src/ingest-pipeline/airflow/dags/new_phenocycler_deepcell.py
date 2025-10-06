import re
import utils

from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from pathlib import Path

from utils import (
    get_queue_resource,
    get_uuid_for_error,
    HMDAG,
    get_tmp_dir_path,
    get_preserve_scratch_resource,
    get_absolute_workflow,
    build_dataset_name as inner_build_dataset_name,
    get_parent_data_dir,
    get_cwl_cmd_from_workflows,
    join_quote_command_str,
)
from hubmap_operators import (
    LogInfoOperator,
    CreateTmpDirOperator,
    FlexMultiDagRunOperator,
)

from extra_utils import build_tag_containers

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
    "on_failure_callback": utils.create_dataset_state_error_callback(get_uuid_for_error),
}

with HMDAG(
    "phenocycler_segmentation",
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
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_cell_count_cmd",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_segmentation",
        },
    )

    prepare_cell_count_cmd = EmptyOperator(task_id="prepare_cell_count_cmd")

    @task(task_id="cell_count_cmd")
    def build_cell_count_cmd(**kwargs):
        tmpdir = get_tmp_dir_path(kwargs["run_id"])
        print("tmpdir: ", tmpdir)
        pattern = r"\: (\d+),$"
        with open(Path(tmpdir, "session.log"), "r") as f:
            for line in f:
                if "num_cells" in line:
                    num_cells = re.search(pattern, line).group(1)
        print("num_cells: ", num_cells)
        kwargs["ti"].xcom_push(key="small_sprm", value=1 if int(num_cells) > 200000 else 0)
        return 0

    cell_count_cmd = build_cell_count_cmd()

    t_maybe_start_small_sprm = BranchPythonOperator(
        task_id="maybe_start_small",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "trigger_phenocycler_small",
            "bail_op": "trigger_phenocycler",
            "test_op": "cell_count_cmd",
            "test_key": "small_sprm",
        },
    )

    def trigger_phenocycler(**kwargs):
        assay_type = kwargs.get("assay_type")
        payload = {
            "tmp_dir": get_tmp_dir_path((kwargs["run_id"])),
        }
        for next_dag in utils.downstream_workflow_iter("generic_metadatatsv", assay_type):
            yield next_dag, payload

    t_trigger_phenocyler_small = FlexMultiDagRunOperator(
        task_id="trigger_phenocycler_small",
        dag=dag,
        trigger_dag_id="phenocycler_segmentation",
        python_callable=trigger_phenocycler,
        op_kwargs={"collection_type": "phenocycler_small",
                   "assay_type": "phenocycler-deepcell"},
    )

    t_trigger_phenocyler = FlexMultiDagRunOperator(
        task_id="trigger_phenocycler",
        dag=dag,
        trigger_dag_id="phenocycler_segmentation",
        python_callable=trigger_phenocycler,
        op_kwargs={"collection_type": "phenocycler",
                   "assay_type": "phenocycler-deep-cell"},
    )

    t_log_info = LogInfoOperator(task_id="log_info")
    t_create_tmpdir = CreateTmpDirOperator(task_id="create_tmpdir")

    (
        t_log_info
        >> t_create_tmpdir

        >> prepare_cwl_segmentation
        >> t_build_cwl_segmentation
        >> t_pipeline_exec_cwl_segmentation
        >> t_maybe_keep_cwl_segmentation

        >> prepare_cell_count_cmd
        >> cell_count_cmd
        >> t_maybe_start_small_sprm

        >> t_trigger_phenocyler_small
    )
    t_maybe_start_small_sprm >> t_trigger_phenocyler
