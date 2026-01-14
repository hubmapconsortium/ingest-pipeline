from pathlib import Path
from datetime import datetime, timedelta

import utils
from extra_utils import build_tag_containers
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task

from hubmap_operators.common_operators import (
    LogInfoOperator,
    JoinOperator,
    CreateTmpDirOperator,
    CleanupTmpDirOperator,
    MoveDataOperator,
)

import utils
from utils import (
    get_dataset_uuid,
    get_absolute_workflow,
    get_parent_dataset_uuid,
    get_parent_dataset_uuids_list,
    get_parent_data_dir,
    build_dataset_name as inner_build_dataset_name,
    get_previous_revision_uuid,
    get_uuid_for_error,
    join_quote_command_str,
    make_send_status_msg_function,
    get_tmp_dir_path,
    HMDAG,
    get_queue_resource,
    get_preserve_scratch_resource,
    get_threads_resource,
    get_cwl_cmd_from_workflows,
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
    "queue": get_queue_resource("codex_cytokit"),
    "on_failure_callback": utils.create_dataset_state_error_callback(get_uuid_for_error),
}

with HMDAG(
    "codex_cytokit",
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args=default_args,
    user_defined_macros={
        "tmp_dir_path": get_tmp_dir_path,
        "preserve_scratch": get_preserve_scratch_resource("codex_cytokit"),
    },
) as dag:
    pipeline_name = "codex-pipeline"
    workflow_version = "1.0.0"
    workflow_description = "The CODEX pipeline performs illumination correction and other pre-processing steps, segments nuclei and cells using Cytokit, and performs spatial analysis of expression data using SPRM, which computes various measures of analyte intensity per cell, performs clustering based on expression and other data, and computes markers for each cluster."
    steps_dir = Path(pipeline_name) / "steps"

    cwl_workflows = [
        {
            "workflow_path": str(
                get_absolute_workflow(steps_dir / "illumination_first_stitching.cwl")
            ),
            "documentation_url": "",
        },
        {
            "workflow_path": str(get_absolute_workflow(steps_dir / "run_cytokit.cwl")),
            "documentation_url": "",
        },
        {
            "workflow_path": str(
                get_absolute_workflow(steps_dir / "ometiff_second_stitching.cwl")
            ),
            "documentation_url": "",
        },
        {
            "workflow_path": str(
                get_absolute_workflow(Path("ribca", "pipeline.cwl"))
            ),
            "documentation_url": "",
        },
        {
            "workflow_path": str(
                get_absolute_workflow(Path("deepcelltypes", "run_deepcelltypes.cwl"))
            ),
            "documentation_url": "",
        },
        {
            "workflow_path": str(
                get_absolute_workflow(Path("stellar", "pipeline.cwl"))
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

    @task(task_id="prepare_cwl_illumination_first_stitching")
    def prepare_cwl_cmd1(**kwargs):
        if kwargs["dag_run"].conf.get("dryrun"):
            cwl_path = Path(cwl_workflows[0]["workflow_path"]).parent
            return build_tag_containers(cwl_path)
        else:
            return "No Container build required"

    prepare_cwl_illumination_first_stitching = prepare_cwl_cmd1()

    def build_cwltool_cwl_illumination_first_stitching(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)
        data_dir = get_parent_data_dir(**kwargs)
        print("data_dir: ", data_dir)

        input_parameters = [
            {"parameter_name": "--gpus", "value": "0,1"},
            {"parameter_name": "--data_dir", "value": str(data_dir)},
        ]

        command = get_cwl_cmd_from_workflows(
            cwl_workflows, 0, input_parameters, tmpdir, kwargs["ti"]
        )

        return join_quote_command_str(command)

    t_build_cwl_illumination_first_stitching = PythonOperator(
        task_id="build_cwl_illumination_first_stitching",
        python_callable=build_cwltool_cwl_illumination_first_stitching,
        provide_context=True,
    )

    t_pipeline_exec_cwl_illumination_first_stitching = BashOperator(
        task_id="pipeline_exec_cwl_illumination_first_stitching",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        mkdir -p ${tmp_dir}/cwl_out ; \
        {{ti.xcom_pull(task_ids='build_cwl_illumination_first_stitching')}} > $tmp_dir/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_maybe_keep_cwl_illumination_first_stitching = BranchPythonOperator(
        task_id="maybe_keep_cwl_illumination_first_stitching",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_cwl_cytokit",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_illumination_first_stitching",
        },
    )

    @task(task_id="prepare_cwl_cytokit")
    def prepare_cwl_cmd2(**kwargs):
        if kwargs["dag_run"].conf.get("dryrun"):
            cwl_path = Path(cwl_workflows[1]["workflow_path"]).parent
            return build_tag_containers(cwl_path)
        else:
            return "No Container build required"

    prepare_cwl_cytokit = prepare_cwl_cmd2()

    def build_cwltool_cwl_cytokit(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)
        data_dir = tmpdir / "cwl_out"
        print("data_dir: ", data_dir)

        workflows = kwargs["ti"].xcom_pull(
            key="cwl_workflows", task_ids="build_cwl_illumination_first_stitching"
        )

        input_parameters = [
            {"parameter_name": "--data_dir", "value": str(data_dir / "new_tiles")},
            {"parameter_name": "--yaml_config", "value": str(data_dir / "experiment.yaml")},
        ]

        command = get_cwl_cmd_from_workflows(workflows, 1, input_parameters, tmpdir, kwargs["ti"])

        return join_quote_command_str(command)

    t_build_cwl_cytokit = PythonOperator(
        task_id="build_cwl_cytokit",
        python_callable=build_cwltool_cwl_cytokit,
        provide_context=True,
    )

    t_pipeline_exec_cwl_cytokit = BashOperator(
        task_id="pipeline_exec_cwl_cytokit",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        {{ti.xcom_pull(task_ids='build_cwl_cytokit')}} >> $tmp_dir/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_maybe_keep_cwl_cytokit = BranchPythonOperator(
        task_id="maybe_keep_cwl_cytokit",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_cwl_ometiff_second_stitching",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_cytokit",
        },
    )

    @task(task_id="prepare_cwl_ometiff_second_stitching")
    def prepare_cwl_cmd3(**kwargs):
        if kwargs["dag_run"].conf.get("dryrun"):
            cwl_path = Path(cwl_workflows[2]["workflow_path"]).parent
            return build_tag_containers(cwl_path)
        else:
            return "No Container build required"

    prepare_cwl_ometiff_second_stitching = prepare_cwl_cmd3()

    def build_cwltool_cwl_ometiff_second_stitching(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)
        data_dir = tmpdir / "cwl_out"
        print("data_dir: ", data_dir)

        workflows = kwargs["ti"].xcom_pull(key="cwl_workflows", task_ids="build_cwl_cytokit")

        input_parameters = [
            {"parameter_name": "--cytokit_config", "value": str(data_dir / "experiment.yaml")},
            {"parameter_name": "--cytokit_output", "value": str(data_dir / "cytokit")},
            {
                "parameter_name": "--slicing_pipeline_config",
                "value": str(data_dir / "pipelineConfig.json"),
            },
            {
                "parameter_name": "--num_concurrent_tasks",
                "value": get_threads_resource(dag.dag_id),
            },
            {"parameter_name": "--data_dir", "value": str(get_parent_data_dir(**kwargs))},
        ]
        command = get_cwl_cmd_from_workflows(workflows, 2, input_parameters, tmpdir, kwargs["ti"])

        return join_quote_command_str(command)

    t_build_cwl_ometiff_second_stitching = PythonOperator(
        task_id="build_cwl_ometiff_second_stitching",
        python_callable=build_cwltool_cwl_ometiff_second_stitching,
        provide_context=True,
    )

    t_pipeline_exec_cwl_ometiff_second_stitching = BashOperator(
        task_id="pipeline_exec_cwl_ometiff_second_stitching",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        {{ti.xcom_pull(task_ids='build_cwl_ometiff_second_stitching')}} >> $tmp_dir/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_maybe_keep_cwl_ometiff_second_stitching = BranchPythonOperator(
        task_id="maybe_keep_cwl_ometiff_second_stitching",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_cwl_ribca",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_ometiff_second_stitching",
        },
    )

    @task(task_id="prepare_cwl_ribca")
    def prepare_cwl_cmd4(**kwargs):
        if kwargs["dag_run"].conf.get("dryrun"):
            cwl_path = Path(cwl_workflows[3]["workflow_path"]).parent
            return build_tag_containers(cwl_path)
        else:
            return "No Container build required"

    prepare_cwl_ribca = prepare_cwl_cmd4()

    def build_cwltool_cwl_ribca(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)
        parent_data_dir = get_parent_data_dir(**kwargs)
        print("parent_data_dir: ", parent_data_dir)
        data_dir = tmpdir / "cwl_out"
        print("data_dir: ", data_dir)

        workflows = kwargs["ti"].xcom_pull(
            key="cwl_workflows", task_ids="build_cwl_ometiff_second_stitching"
        )

        input_parameters = [{"parameter_name": "--data_dir", "value": str(data_dir)}]
        command = get_cwl_cmd_from_workflows(workflows, 3, input_parameters, tmpdir, kwargs["ti"])

        return join_quote_command_str(command)

    t_build_cmd_ribca = PythonOperator(
        task_id="build_cwl_ribca",
        python_callable=build_cwltool_cwl_ribca,
        provide_context=True,
    )

    t_pipeline_exec_cwl_ribca = BashOperator(
        task_id="pipeline_exec_cwl_ribca",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        {{ti.xcom_pull(task_ids='build_cwl_ribca')}} >> ${tmp_dir}/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_maybe_keep_cwl_ribca = BranchPythonOperator(
        task_id="maybe_keep_cwl_ribca",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_cwl_deepcelltypes",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_ribca",
        },
    )

    t_delete_internal_pipeline_files = BashOperator(
        task_id="delete_internal_pipeline_files",
        bash_command="""\
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        cd "${tmp_dir}"/cwl_out ; \
        rm -rf cytokit new_tiles
        """,
    )

    @task(task_id="prepare_cwl_deepcelltypes")
    def prepare_cwl_cmd5(**kwargs):
        if kwargs["dag_run"].conf.get("dryrun"):
            cwl_path = Path(cwl_workflows[4]["workflow_path"]).parent
            return build_tag_containers(cwl_path)
        else:
            return "No Container build required"

    prepare_cwl_deepcelltypes = prepare_cwl_cmd5()

    def build_cwltool_cmd_deepcelltypes(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)
        parent_data_dir = get_parent_data_dir(**kwargs)
        print("parent_data_dir: ", parent_data_dir)
        data_dir = tmpdir / "cwl_out"
        print("data_dir: ", data_dir)

        workflows = kwargs["ti"].xcom_pull(key="cwl_workflows", task_ids="build_cwl_ribca")

        input_parameters = [
            {"parameter_name": "--data_dir", "value": str(data_dir)},
        ]

        command = get_cwl_cmd_from_workflows(workflows, 4, input_parameters, tmpdir, kwargs["ti"])

        return join_quote_command_str(command)

    t_build_cmd_deepcelltypes = PythonOperator(
        task_id="build_cmd_deepcelltypes",
        python_callable=build_cwltool_cmd_deepcelltypes,
        provide_context=True,
    )

    t_pipeline_exec_cwl_deepcelltypes = BashOperator(
        task_id="pipeline_exec_cwl_deepcelltypes",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        {{ti.xcom_pull(task_ids='build_cmd_deepcelltypes')}} >> ${tmp_dir}/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_maybe_keep_cwl_deepcelltypes = BranchPythonOperator(
        task_id="maybe_keep_cwl_deepcelltypes",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_cwl_stellar",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_deepcelltypes",
        },
    )

    @task(task_id="prepare_cwl_stellar")
    def prepare_cwl_cmd6(**kwargs):
        if kwargs["dag_run"].conf.get("dryrun"):
            cwl_path = Path(cwl_workflows[5]["workflow_path"]).parent
            return build_tag_containers(cwl_path)
        else:
            return "No Container build required"

    prepare_cwl_stellar = prepare_cwl_cmd6()

    def build_cwltool_cmd_stellar(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)
        parent_data_dir = get_parent_data_dir(**kwargs)
        print("parent_data_dir: ", parent_data_dir)
        data_dir = tmpdir / "cwl_out"
        print("data_dir: ", data_dir)

        workflows = kwargs["ti"].xcom_pull(
            key="cwl_workflows", task_ids="build_cmd_deepcelltypes"
        )

        input_parameters = [
            {"parameter_name": "--data_dir", "value": str(data_dir)},
        ]

        command = get_cwl_cmd_from_workflows(workflows, 5, input_parameters, tmpdir, kwargs["ti"])

        return join_quote_command_str(command)

    t_build_cmd_stellar = PythonOperator(
        task_id="build_cmd_stellar",
        python_callable=build_cwltool_cmd_stellar,
        provide_context=True,
    )

    t_pipeline_exec_cwl_stellar = BashOperator(
        task_id="pipeline_exec_cwl_stellar",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        {{ti.xcom_pull(task_ids='build_cmd_stellar')}} >> ${tmp_dir}/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_maybe_keep_cwl_stellar = BranchPythonOperator(
        task_id="maybe_keep_cwl_stellar",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_cwl_sprm",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_stellar",
        },
    )

    prepare_cwl_sprm = EmptyOperator(task_id="prepare_cwl_sprm")

    def build_cwltool_cmd_sprm(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)
        parent_data_dir = get_parent_data_dir(**kwargs)
        print("parent_data_dir: ", parent_data_dir)
        data_dir = tmpdir / "cwl_out"
        print("data_dir: ", data_dir)

        workflows = kwargs["ti"].xcom_pull(key="cwl_workflows", task_ids="build_cmd_deepcelltypes")

        input_parameters = [
            {"parameter_name": "--enable_manhole", "value": ""},
            {"parameter_name": "--processes", "value": get_threads_resource(dag.dag_id)},
            {"parameter_name": "--image_dir", "value": str(data_dir / "pipeline_output/expr")},
            {"parameter_name": "--mask_dir", "value": str(data_dir / "pipeline_output/mask")},
            {"parameter_name": "--cell_types_directory", "value": str(data_dir / "ribca_for_sprm")},
            {"parameter_name": "--cell_types_directory", "value": str(data_dir / "deepcelltypes")},
            {"parameter_name": "--cell_types_directory", "value": str(data_dir / "stellar")},
        ]

        command = get_cwl_cmd_from_workflows(workflows, 6, input_parameters, tmpdir, kwargs["ti"])

        return join_quote_command_str(command)

    t_build_cmd_sprm = PythonOperator(
        task_id="build_cmd_sprm",
        python_callable=build_cwltool_cmd_sprm,
        provide_context=True,
    )

    t_pipeline_exec_cwl_sprm = BashOperator(
        task_id="pipeline_exec_cwl_sprm",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        {{ti.xcom_pull(task_ids='build_cmd_sprm')}} >> ${tmp_dir}/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_maybe_keep_cwl_sprm = BranchPythonOperator(
        task_id="maybe_keep_cwl_sprm",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_cwl_create_vis_symlink_archive",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_sprm",
        },
    )

    prepare_cwl_create_vis_symlink_archive = EmptyOperator(
        task_id="prepare_cwl_create_vis_symlink_archive",
    )

    def build_cwltool_cmd_create_vis_symlink_archive(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)
        parent_data_dir = get_parent_data_dir(**kwargs)
        print("parent_data_dir: ", parent_data_dir)
        data_dir = tmpdir / "cwl_out"
        print("data_dir: ", data_dir)

        workflows = kwargs["ti"].xcom_pull(key="cwl_workflows", task_ids="build_cmd_sprm")

        input_parameters = [
            {"parameter_name": "--ometiff_dir", "value": str(data_dir / "pipeline_output")},
            {"parameter_name": "--sprm_output", "value": str(data_dir / "sprm_outputs")},
        ]
        command = get_cwl_cmd_from_workflows(workflows, 7, input_parameters, tmpdir, kwargs["ti"])

        return join_quote_command_str(command)

    t_build_cmd_create_vis_symlink_archive = PythonOperator(
        task_id="build_cmd_create_vis_symlink_archive",
        python_callable=build_cwltool_cmd_create_vis_symlink_archive,
        provide_context=True,
    )

    t_pipeline_exec_cwl_create_vis_symlink_archive = BashOperator(
        task_id="pipeline_exec_cwl_create_vis_symlink_archive",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        {{ti.xcom_pull(task_ids='build_cmd_create_vis_symlink_archive')}} >> ${tmp_dir}/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_maybe_keep_cwl_create_vis_symlink_archive = BranchPythonOperator(
        task_id="maybe_keep_cwl_create_vis_symlink_archive",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_cwl_ome_tiff_pyramid",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_create_vis_symlink_archive",
        },
    )

    prepare_cwl_ome_tiff_pyramid = EmptyOperator(task_id="prepare_cwl_ome_tiff_pyramid")

    def build_cwltool_cwl_ome_tiff_pyramid(**kwargs):
        run_id = kwargs["run_id"]

        # tmpdir is temp directory in /hubmap-tmp
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)

        # data directory is the stitched images, which are found in tmpdir
        data_dir = get_parent_data_dir(**kwargs)
        print("data_dir: ", data_dir)

        workflows = kwargs["ti"].xcom_pull(
            key="cwl_workflows", task_ids="build_cmd_create_vis_symlink_archive"
        )

        input_parameters = [
            {"parameter_name": "--processes", "value": get_threads_resource(dag.dag_id)},
            {"parameter_name": "--ometiff_directory", "value": str(tmpdir / "cwl_out")},
        ]
        command = get_cwl_cmd_from_workflows(workflows, 8, input_parameters, tmpdir, kwargs["ti"])

        return join_quote_command_str(command)

    t_build_cmd_ome_tiff_pyramid = PythonOperator(
        task_id="build_cwl_ome_tiff_pyramid",
        python_callable=build_cwltool_cwl_ome_tiff_pyramid,
        provide_context=True,
    )

    t_pipeline_exec_cwl_ome_tiff_pyramid = BashOperator(
        task_id="pipeline_exec_cwl_ome_tiff_pyramid",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        {{ti.xcom_pull(task_ids='build_cwl_ome_tiff_pyramid')}} >> $tmp_dir/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_maybe_keep_cwl_ome_tiff_pyramid = BranchPythonOperator(
        task_id="maybe_keep_cwl_ome_tiff_pyramid",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_cwl_ome_tiff_offsets",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_ome_tiff_pyramid",
        },
    )

    prepare_cwl_ome_tiff_offsets = EmptyOperator(task_id="prepare_cwl_ome_tiff_offsets")

    def build_cwltool_cmd_ome_tiff_offsets(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)
        parent_data_dir = get_parent_data_dir(**kwargs)
        print("parent_data_dir: ", parent_data_dir)
        data_dir = tmpdir / "cwl_out"
        print("data_dir: ", data_dir)

        workflows = kwargs["ti"].xcom_pull(
            key="cwl_workflows", task_ids="build_cwl_ome_tiff_pyramid"
        )

        input_parameters = [
            {"parameter_name": "--input_dir", "value": str(data_dir / "ometiff-pyramids")},
        ]
        command = get_cwl_cmd_from_workflows(workflows, 9, input_parameters, tmpdir, kwargs["ti"])

        return join_quote_command_str(command)

    t_build_cmd_ome_tiff_offsets = PythonOperator(
        task_id="build_cmd_ome_tiff_offsets",
        python_callable=build_cwltool_cmd_ome_tiff_offsets,
        provide_context=True,
    )

    t_pipeline_exec_cwl_ome_tiff_offsets = BashOperator(
        task_id="pipeline_exec_cwl_ome_tiff_offsets",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        {{ti.xcom_pull(task_ids='build_cmd_ome_tiff_offsets')}} >> ${tmp_dir}/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_maybe_keep_cwl_ome_tiff_offsets = BranchPythonOperator(
        task_id="maybe_keep_cwl_ome_tiff_offsets",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_cwl_sprm_to_json",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_ome_tiff_offsets",
        },
    )

    prepare_cwl_sprm_to_json = EmptyOperator(task_id="prepare_cwl_sprm_to_json")

    def build_cwltool_cmd_sprm_to_json(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)
        parent_data_dir = get_parent_data_dir(**kwargs)
        print("parent_data_dir: ", parent_data_dir)
        data_dir = tmpdir / "cwl_out"  # This stage reads input from stage 1
        print("data_dir: ", data_dir)

        workflows = kwargs["ti"].xcom_pull(
            key="cwl_workflows", task_ids="build_cmd_ome_tiff_offsets"
        )

        input_parameters = [
            {"parameter_name": "--input_dir", "value": str(data_dir / "sprm_outputs")},
        ]
        command = get_cwl_cmd_from_workflows(workflows, 10, input_parameters, tmpdir, kwargs["ti"])

        return join_quote_command_str(command)

    t_build_cmd_sprm_to_json = PythonOperator(
        task_id="build_cmd_sprm_to_json",
        python_callable=build_cwltool_cmd_sprm_to_json,
        provide_context=True,
    )

    t_pipeline_exec_cwl_sprm_to_json = BashOperator(
        task_id="pipeline_exec_cwl_sprm_to_json",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        {{ti.xcom_pull(task_ids='build_cmd_sprm_to_json')}} >> ${tmp_dir}/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_maybe_keep_cwl_sprm_to_json = BranchPythonOperator(
        task_id="maybe_keep_cwl_sprm_to_json",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_cwl_sprm_to_anndata",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_sprm_to_json",
        },
    )

    prepare_cwl_sprm_to_anndata = EmptyOperator(task_id="prepare_cwl_sprm_to_anndata")

    def build_cwltool_cmd_sprm_to_anndata(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)
        parent_data_dir = get_parent_data_dir(**kwargs)
        print("parent_data_dir: ", parent_data_dir)
        data_dir = tmpdir / "cwl_out"  # This stage reads input from stage 1
        print("data_dir: ", data_dir)

        workflows = kwargs["ti"].xcom_pull(key="cwl_workflows", task_ids="build_cmd_sprm_to_json")

        input_parameters = [
            {"parameter_name": "--input_dir", "value": str(data_dir / "sprm_outputs")},
        ]

        command = get_cwl_cmd_from_workflows(workflows, 11, input_parameters, tmpdir, kwargs["ti"])

        return join_quote_command_str(command)

    t_build_cmd_sprm_to_anndata = PythonOperator(
        task_id="build_cmd_sprm_to_anndata",
        python_callable=build_cwltool_cmd_sprm_to_anndata,
        provide_context=True,
    )

    t_pipeline_exec_cwl_sprm_to_anndata = BashOperator(
        task_id="pipeline_exec_cwl_sprm_to_anndata",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        {{ti.xcom_pull(task_ids='build_cmd_sprm_to_anndata')}} >> ${tmp_dir}/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_maybe_keep_cwl_sprm_to_anndata = BranchPythonOperator(
        task_id="maybe_keep_cwl_sprm_to_anndata",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "maybe_create_dataset",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_sprm_to_anndata",
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

    t_send_create_dataset = PythonOperator(
        task_id="send_create_dataset",
        python_callable=utils.pythonop_send_create_dataset,
        provide_context=True,
        op_kwargs={
            "parent_dataset_uuid_callable": get_parent_dataset_uuids_list,
            "previous_revision_uuid_callable": get_previous_revision_uuid,
            "http_conn_id": "ingest_api_connection",
            "dataset_name_callable": build_dataset_name,
            "pipeline_shorthand": "Cytokit + SPRM",
        },
    )

    t_set_dataset_error = PythonOperator(
        task_id="set_dataset_error",
        python_callable=utils.pythonop_set_dataset_state,
        provide_context=True,
        trigger_rule="all_done",
        op_kwargs={
            "dataset_uuid_callable": get_dataset_uuid,
            "ds_state": "Error",
            "message": "An error occurred in {}".format(pipeline_name),
            "parent_dataset_uuid_callable": get_parent_dataset_uuid,
            "pipeline_name": pipeline_name
        },
    )

    t_expand_symlinks = BashOperator(
        task_id="expand_symlinks",
        bash_command="""
        tmp_dir="{{tmp_dir_path(run_id)}}" ; \
        ds_dir="{{ti.xcom_pull(task_ids='send_create_dataset')}}" ; \
        groupname="{{conf.as_dict()['connections']['OUTPUT_GROUP_NAME']}}" ; \
        cd "$ds_dir" ; \
        tar -xf symlinks.tar ; \
        echo $?
        """,
    )

    send_status_msg = make_send_status_msg_function(
        dag_file=__file__,
        retcode_ops=[
            "pipeline_exec_cwl_illumination_first_stitching",
            "pipeline_exec_cwl_cytokit",
            "pipeline_exec_cwl_ometiff_second_stitching",
            "pipeline_exec_cwl_ribca",
            "pipeline_exec_cwl_deepcelltypes",
            "pipeline_exec_cwl_stellar",
            "pipeline_exec_cwl_sprm",
            "pipeline_exec_cwl_create_vis_symlink_archive",
            "pipeline_exec_cwl_ome_tiff_offsets",
            "pipeline_exec_cwl_sprm_to_json",
            "pipeline_exec_cwl_sprm_to_anndata",
            "move_data",
        ],
        cwl_workflows=lambda **kwargs: kwargs["ti"].xcom_pull(
            key="cwl_workflows", task_ids="build_cmd_sprm_to_anndata"
        ),
        workflow_description=workflow_description,
        workflow_version=workflow_version,
    )

    t_send_status = PythonOperator(
        task_id="send_status_msg", python_callable=send_status_msg, provide_context=True
    )

    t_log_info = LogInfoOperator(task_id="log_info")
    t_join = JoinOperator(task_id="join")
    t_create_tmpdir = CreateTmpDirOperator(task_id="create_tmpdir")
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id="cleanup_tmpdir")
    t_move_data = MoveDataOperator(task_id="move_data")

    (
        t_log_info
        >> t_create_tmpdir

        >> prepare_cwl_illumination_first_stitching
        >> t_build_cwl_illumination_first_stitching
        >> t_pipeline_exec_cwl_illumination_first_stitching
        >> t_maybe_keep_cwl_illumination_first_stitching

        >> prepare_cwl_cytokit
        >> t_build_cwl_cytokit
        >> t_pipeline_exec_cwl_cytokit
        >> t_maybe_keep_cwl_cytokit

        >> prepare_cwl_ometiff_second_stitching
        >> t_build_cwl_ometiff_second_stitching
        >> t_pipeline_exec_cwl_ometiff_second_stitching
        >> t_delete_internal_pipeline_files
        >> t_maybe_keep_cwl_ometiff_second_stitching

        >> prepare_cwl_ribca
        >> t_build_cmd_ribca
        >> t_pipeline_exec_cwl_ribca
        >> t_maybe_keep_cwl_ribca

        >> prepare_cwl_deepcelltypes
        >> t_build_cmd_deepcelltypes
        >> t_pipeline_exec_cwl_deepcelltypes
        >> t_maybe_keep_cwl_deepcelltypes

        >> prepare_cwl_stellar
        >> t_build_cmd_stellar
        >> t_pipeline_exec_cwl_stellar
        >> t_maybe_keep_cwl_stellar

        >> prepare_cwl_sprm
        >> t_build_cmd_sprm
        >> t_pipeline_exec_cwl_sprm
        >> t_maybe_keep_cwl_sprm

        >> prepare_cwl_create_vis_symlink_archive
        >> t_build_cmd_create_vis_symlink_archive
        >> t_pipeline_exec_cwl_create_vis_symlink_archive
        >> t_maybe_keep_cwl_create_vis_symlink_archive

        >> prepare_cwl_ome_tiff_pyramid
        >> t_build_cmd_ome_tiff_pyramid
        >> t_pipeline_exec_cwl_ome_tiff_pyramid
        >> t_maybe_keep_cwl_ome_tiff_pyramid

        >> prepare_cwl_ome_tiff_offsets
        >> t_build_cmd_ome_tiff_offsets
        >> t_pipeline_exec_cwl_ome_tiff_offsets
        >> t_maybe_keep_cwl_ome_tiff_offsets

        >> prepare_cwl_sprm_to_json
        >> t_build_cmd_sprm_to_json
        >> t_pipeline_exec_cwl_sprm_to_json
        >> t_maybe_keep_cwl_sprm_to_json

        >> prepare_cwl_sprm_to_anndata
        >> t_build_cmd_sprm_to_anndata
        >> t_pipeline_exec_cwl_sprm_to_anndata
        >> t_maybe_keep_cwl_sprm_to_anndata
        >> t_maybe_create_dataset

        >> t_send_create_dataset
        >> t_move_data
        >> t_expand_symlinks
        >> t_send_status
        >> t_join
    )
    t_maybe_keep_cwl_illumination_first_stitching >> t_set_dataset_error
    t_maybe_keep_cwl_cytokit >> t_set_dataset_error
    t_maybe_keep_cwl_ometiff_second_stitching >> t_set_dataset_error
    t_maybe_keep_cwl_ribca >> t_set_dataset_error
    t_maybe_keep_cwl_deepcelltypes >> t_set_dataset_error
    t_maybe_keep_cwl_sprm >> t_set_dataset_error
    t_maybe_keep_cwl_create_vis_symlink_archive >> t_set_dataset_error
    t_maybe_keep_cwl_ome_tiff_pyramid >> t_set_dataset_error
    t_maybe_keep_cwl_ome_tiff_offsets >> t_set_dataset_error
    t_maybe_keep_cwl_sprm_to_json >> t_set_dataset_error
    t_maybe_keep_cwl_sprm_to_anndata >> t_set_dataset_error
    t_set_dataset_error >> t_join
    t_maybe_create_dataset >> t_join
    t_join >> t_cleanup_tmpdir
