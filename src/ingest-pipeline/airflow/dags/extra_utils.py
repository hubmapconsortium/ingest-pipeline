import json
from typing import List, Dict
from pathlib import Path
from csv import DictReader

from airflow.providers.http.hooks.http import HttpHook
from pprint import pprint
from typing import Tuple


def check_link_published_drvs(uuid: str, auth_tok: str) -> Tuple[bool, str]:
    needs_previous_version = False
    published_uuid = ""
    endpoint = f"/children/{uuid}"
    headers = {
        "content-type": "application/json",
        "X-Hubmap-Application": "ingest-pipeline",
        "Authorization": f"Bearer {auth_tok}"
    }
    extra_options = {}

    http_hook = HttpHook("GET", http_conn_id="entity_api_connection")

    response = http_hook.run(endpoint, headers=headers, extra_options=extra_options)
    print("response: ")
    pprint(response.json())
    for data in response.json():
        if (
            data.get("entity_type") in ("Dataset", "Publication")
            and data.get("status") == "Published"
        ):
            needs_previous_version = True
            published_uuid = data.get("uuid")
    return needs_previous_version, published_uuid


def get_component_uuids(uuid:str, auth_tok: str) -> List:
    children = []
    endpoint = f"/children/{uuid}"
    headers = {
        "content-type": "application/json",
        "X-Hubmap-Application": "ingest-pipeline",
        "Authorization": f"Bearer {auth_tok}"
    }
    extra_options = {}

    http_hook = HttpHook("GET", http_conn_id="entity_api_connection")

    response = http_hook.run(endpoint, headers=headers, extra_options=extra_options)
    print("response: ")
    pprint(response.json())
    for data in response.json():
        if data.get("creation_action") == "Multi-Assay Split":
            children.append(data.get("uuid"))
    return children


class SoftAssayClient:
    def __init__(self, metadata_files: List, auth_tok: str):
        self.assay_components = []
        self.primary_assay = {}
        self.is_multiassay = True
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
            }
            if not assay_type.get("must-contain"):
                print(f"Component {assay_type}")
                self.assay_components.append(data_component)
            else:
                print(f"Primary {assay_type}")
                self.primary_assay = data_component
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
