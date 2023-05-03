import sys

import json
from pathlib import Path
from datetime import datetime, timedelta
from pprint import pprint
from collections import defaultdict

import frontmatter

from airflow.configuration import conf as airflow_conf
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook

from hubmap_operators.common_operators import (
    CreateTmpDirOperator,
    CleanupTmpDirOperator,
)

from utils import (
    localized_assert_json_matches_schema as assert_json_matches_schema,
    encrypt_tok,
    get_tmp_dir_path,
    get_auth_tok,
    pythonop_get_dataset_state,
    HMDAG,
    get_queue_resource,
    get_preserve_scratch_resource,
    get_threads_resource,
    get_uuid_for_error,
    create_dataset_state_error_callback,
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

        for key in ["entity_type", "status", "uuid", "data_types", "local_directory_full_path"]:
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

    def build_ancillary_data(**kwargs):
        uuid = kwargs["ti"].xcom_pull(key="uuid")
        lz_path = kwargs["ti"].xcom_pull(key="lz_path")
        top_path = Path(lz_path)
        vignette_path = top_path / 'vignettes'
        assert vignette_path.is_dir(), 'vignettes not found'
        pub_path = top_path / 'publication.md'
        assert pub_path.is_file(), 'publication.md not found'
        rslt = {}
        pub_fm = frontmatter.loads(pub_path.read_text())
        pub_md = pub_fm.metadata
        print('publication metadata follows')
        pprint(pub_md)
        pub_content = pub_fm.content
        print('publication content follows')
        pprint(pub_content)
        rslt['vignettes'] = []
        rslt['description'] = pub_content
        assert 'name' in pub_md, 'description metadata does not include a name'
        rslt['name'] = pub_md['name']
        if 'figures' in pub_md:
            fig_list = pub_md['figures']
        else:
            fig_list = []
        all_figs = defaultdict(list)
        for fig_d in fig_list:
            assert 'name' in fig_d, 'description figure has no name'
            assert 'file' in fig_d, f"figure {fig_d['name']} has no json block"
            fig_json_path = pub_path.parent / fig_d['file']
            assert fig_json_path.suffix == '.json', f'figure {fig_json_path} is not json'
            rel_path = fig_json_path.relative_to(vignette_path)
            vig_path = rel_path.parents[0]
            all_figs[vig_path].append({
                'name': fig_d['name'],
                'file': rel_path.relative_to(vig_path)
            })
        print('all_figs follows')
        pprint(all_figs)
        
        for this_vignette_path in vignette_path.glob('*'):
            print(f'this_vignette_path {this_vignette_path}')
            assert this_vignette_path.is_dir(), 'Found a non-dir in vignettes'
            print(f'looking up {this_vignette_path.name} in {all_figs.keys()}')
            this_vignette_figs = all_figs.get(Path(this_vignette_path.name), [])
            print('this_vignette_figs follows')
            pprint(this_vignette_figs)
            this_vignette_json_names = {elt['file'].name: elt['name'] for elt in this_vignette_figs}
            print('this_vignette_json_names follows')
            pprint(this_vignette_json_names)
            this_description = None
            this_block = {
                'directory_name': this_vignette_path.name,
                'figures': []
            }
            for file_path in this_vignette_path.glob('*'):
                print(f'file in this vignette: {file_path}')
                assert file_path.is_file(), 'Found a subdir in a vignette'
                if file_path.suffix == '.md':
                    assert this_description is None, f"vignette has two descriptions"
                    vig_fm = frontmatter.loads(file_path.read_text())
                    # Any metadata is ignored
                    print('vig_fm metadata follows')
                    pprint(vig_fm.metadata)
                    this_block['name'] = vig_fm.metadata['name']
                    this_block['description'] = vig_fm.content
                else:
                    print(f'check this_vignette_figs for {file_path}')
                    assert file_path.name in this_vignette_json_names, f"{file_path.name} is misplaced"
                    this_block['figures'].append({
                        'name': this_vignette_json_names[file_path.name],
                        'file': file_path.name
                    })
                    
            rslt['vignettes'].append(this_block)
                
        print('rslt follows')
        pprint(rslt)
        assert_json_matches_schema(rslt, 'publication_ancillary_schema.yml')
        return rslt

    t_build_ancillary_data = PythonOperator(
        task_id="build_ancillary_data",
        python_callable=build_ancillary_data,
        provide_context=True,
    )

    def send_status_msg(**kwargs):
        #validation_file_path = Path(kwargs["ti"].xcom_pull(key="validation_file_path"))
        uuid = kwargs["ti"].xcom_pull(key="uuid")
        endpoint = f"/entities/{uuid}"
        headers = {
            "authorization": "Bearer " + get_auth_tok(**kwargs),
            "X-Hubmap-Application": "ingest-pipeline",
            "content-type": "application/json",
        }
        extra_options = []
        http_conn_id = "entity_api_connection"
        http_hook = HttpHook("PUT", http_conn_id=http_conn_id)
        # with open(validation_file_path) as f:
        #     report_txt = f.read()
        # if report_txt.startswith("No errors!"):
        #     data = {
        #         "status": "Valid",
        #     }
        # else:
        #     data = {"status": "Invalid", "validation_message": report_txt}
        #     context = kwargs["ti"].get_template_context()
        #     ValidateUploadFailure(context, execute_methods=False).send_failure_email(
        #         report_txt=report_txt
        #     )
        # print("data: ")
        # pprint(data)
        # response = http_hook.run(
        #     endpoint,
        #     json.dumps(data),
        #     headers,
        #     extra_options,
        # )
        # print("response: ")
        # pprint(response.json())

    t_send_status = PythonOperator(
        task_id="send_status",
        python_callable=send_status_msg,
        provide_context=True,
    )

    t_create_tmpdir = CreateTmpDirOperator(task_id="create_temp_dir")
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id="cleanup_temp_dir")

    t_create_tmpdir >> t_find_uuid >> t_build_ancillary_data >> t_send_status >> t_cleanup_tmpdir
