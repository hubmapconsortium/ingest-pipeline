import json
import pandas as pd
import re

from typing import List, Dict
from pathlib import Path
from csv import DictReader

from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime, timedelta
from pprint import pprint
from typing import Tuple
from multi_docker_build.build_docker_images import build as docker_builder
from hubmap_pipeline_release_mgmt.tag_release_pipeline import adjust_cwl_docker_tags


def check_link_published_drvs(uuid: str, auth_tok: str) -> Tuple[bool, str]:
    needs_previous_version = False
    published_uuid = ""
    endpoint = f"/children/{uuid}"
    headers = {
        "content-type": "application/json",
        "X-Hubmap-Application": "ingest-pipeline",
        "Authorization": f"Bearer {auth_tok}",
    }
    extra_options = {}

    http_hook = HttpHook("GET", http_conn_id="entity_api_connection")

    response = http_hook.run(endpoint, headers=headers, extra_options=extra_options)
    print("response: ")
    pprint(response.json())
    for data in response.json():
        if data.get("entity_type") == "Dataset" and data.get("status") == "Published":
            needs_previous_version = True
            published_uuid = data.get("uuid")
    return needs_previous_version, published_uuid


def get_components(uuid: str, auth_tok: str) -> List:
    children = []
    endpoint = f"/children/{uuid}"
    headers = {
        "content-type": "application/json",
        "X-Hubmap-Application": "ingest-pipeline",
        "Authorization": f"Bearer {auth_tok}",
    }
    extra_options = {}

    http_hook = HttpHook("GET", http_conn_id="entity_api_connection")

    response = http_hook.run(endpoint, headers=headers, extra_options=extra_options)
    print("response: ")
    pprint(response.json())
    for data in response.json():
        if data.get("creation_action") == "Multi-Assay Split":
            children.append(data)
    return children


class SoftAssayClient:
    def __init__(self, metadata_files: List, auth_tok: str):
        self.assay_components = []
        self.primary_assay = {}
        self.is_multiassay = True
        self.is_epic = False
        for metadata_file in metadata_files:
            try:
                rows = self.__read_rows(metadata_file, encoding="UTF-8")
            except Exception as e:
                print(f"Error {e} reading metadata {metadata_file}")
                return
            assay_type = self.__get_assaytype_data(row=rows[0], auth_tok=auth_tok)
            data_component = {
                "assaytype": assay_type.get("assaytype"),
                "dataset-type": assay_type.get("dataset-type"),
                "contains-pii": assay_type.get("contains-pii", True),
                "primary": assay_type.get("primary", False),
                "metadata-file": metadata_file,
                "is-epic": True if assay_type.get("process_state") == "epic" else False,
            }
            if not assay_type.get("must-contain"):
                print(f"Component {assay_type}")
                self.assay_components.append(data_component)
            else:
                print(f"Primary {assay_type}")
                self.primary_assay = data_component
            if data_component.get("is-epic"):
                print(f"EPIC {assay_type}")
                self.is_epic = True
        if not self.primary_assay and len(self.assay_components) == 1:
            self.primary_assay = self.assay_components.pop()
            self.is_multiassay = False

    def __get_assaytype_data(
        self,
        row: Dict,
        auth_tok: str,
    ) -> Dict:
        http_hook = HttpHook("POST", http_conn_id="ingest_api_connection")
        endpoint = f"/assaytype"
        headers = {
            "Authorization": f"Bearer {auth_tok}",
            "Content-Type": "application/json",
        }
        response = http_hook.run(endpoint=endpoint, headers=headers, data=json.dumps(row))
        response.raise_for_status()
        return response.json()

    def __get_context_of_decode_error(self, e: UnicodeDecodeError) -> str:
        buffer = 20
        codec = "latin-1"  # This is not the actual codec of the string!
        before = e.object[max(e.start - buffer, 0) : max(e.start, 0)].decode(codec)  # noqa
        problem = e.object[e.start : e.end].decode(codec)  # noqa
        after = e.object[e.end : min(e.end + buffer, len(e.object))].decode(codec)  # noqa
        in_context = f"{before} [ {problem} ] {after}"
        return f'Invalid {e.encoding} because {e.reason}: "{in_context}"'

    def __dict_reader_wrapper(self, path, encoding: str) -> list:
        with open(path, encoding=encoding) as f:
            rows = list(DictReader(f, dialect="excel-tab"))
        return rows

    def __read_rows(self, path: Path, encoding: str) -> List:
        if not Path(path).exists():
            message = {"File does not exist": f"{path}"}
            raise message
        try:
            rows = self.__dict_reader_wrapper(path, encoding)
            if not rows:
                message = {"File has no data rows": f"{path}"}
            else:
                return rows
        except IsADirectoryError:
            message = {"Expected a TSV, but found a directory": f"{path}"}
        except UnicodeDecodeError as e:
            message = {"Decode Error": self.__get_context_of_decode_error(e)}
        raise message


def build_tag_containers(cwl_path: Path) -> str:
    try:
        docker_builder(
            tag_timestamp=False,
            tag_git_describe=False,
            tag="airflow-devel",
            push=False,
            ignore_missing_submodules=True,
            pretend=False,
            base_dir=cwl_path,
        )
    except Exception as e:
        return f"Error in docker builder: {e}"
    try:
        adjust_cwl_docker_tags(tag_without_v="airflow-devel", base_dir=cwl_path)
    except Exception as e:
        return f"Error adjusting docker tags: {e}"
    return f"Container built for {cwl_path}"


def __get_timestamp(line: str) -> datetime:
    timestamp_format = "%Y-%m-%d %H:%M:%S"
    timestamp_str = line[6:25]
    return datetime.strptime(timestamp_str, timestamp_format)


def __calculate_usage(starting_timestamp: datetime, ending_timestamp: datetime,
                      cpu_count: int) -> timedelta:
    return (ending_timestamp - starting_timestamp) * int(cpu_count)


def calculate_statistics(file_path: str) -> pd:
    df = pd.read_csv(Path(file_path))
    startjob = r"\[job .+\] .+ docker \\$"
    endjob = r"\[job .+\] completed success$"
    processes = r"--num_concurrent_tasks \\$|--processes \\$|--threads \\$"
    single_line = r"^\s{4}[0-9]+$"
    gpu_task = r".*gpu.*"
    gpu = False
    processes_marker = False
    cpu_count = 1
    starting_timestamp = None
    ending_timestamp = None
    df['cpu_usage'] = None
    df['gpu_usage'] = None
    cpu_usage = timedelta(seconds=0)
    gpu_usage = timedelta(seconds=0)
    for index, row in df.iterrows():
        print(f"UUID: {row['uuid']}")
        path = Path(row.directory + "/session.log")
        try:
            with open(path, "r") as session_file:
                for line in session_file:
                    if re.search(startjob, line):
                        starting_timestamp = __get_timestamp(line)
                        # Check if this is CPU or GPU and create a flag
                    if starting_timestamp and re.search(gpu_task, line):
                        gpu = True
                    if processes_marker or re.search(single_line, line):
                        cpu_count *= int(line.strip("\\\n"))
                        processes_marker = False
                    if starting_timestamp and re.search(processes, line):
                        processes_marker = True
                    if re.search(endjob, line) and starting_timestamp:
                        ending_timestamp = __get_timestamp(line)
                    if starting_timestamp and ending_timestamp:
                        print(f"Starting timestamp: {starting_timestamp}")
                        print(f"ending timestamp: {ending_timestamp}")
                        print(f"Increasing time: {ending_timestamp - starting_timestamp}")
                        # if GPU flag, append to GPU, else append to CPU
                        if gpu:
                            gpu_usage += __calculate_usage(starting_timestamp,
                                                           ending_timestamp,
                                                           1)
                            print(f"GPU: {gpu}")
                        else:
                            cpu_usage += __calculate_usage(starting_timestamp, ending_timestamp,
                                                           cpu_count)
                            print(f"CPU count: {cpu_count}")

                        starting_timestamp = None
                        ending_timestamp = None
                        gpu = False
                        cpu_count = 1
                        processes_marker = False
                        print(f"CPU usage: {cpu_usage}, GPU usage: {gpu_usage}")
        except FileNotFoundError:
            print(f"{path} not found")
        except PermissionError:
            print(f"{path} permission denied")
        except Exception as e:
            print(f"Error {e} in: {path}")
        finally:
            df.loc[index, "gpu_usage"] = gpu_usage
            df.loc[index, "cpu_usage"] = cpu_usage
            gpu_usage = timedelta(seconds=0)
            cpu_usage = timedelta(seconds=0)
            starting_timestamp = None
            ending_timestamp = None
            gpu = False
            cpu_count = 1
            processes_marker = False
    return df
