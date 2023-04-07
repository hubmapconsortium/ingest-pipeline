from requests.exceptions import HTTPError
from requests import codes
import traceback

from airflow.configuration import conf as airflow_conf
from airflow.hooks.http_hook import HttpHook

from export_and_backup.export_and_backup_plugin import ExportAndBackupPlugin, add_path
from utils import get_auth_tok

with add_path(airflow_conf.as_dict()["connections"]["SRC_PATH"].strip("'").strip('"')):
    from submodules import hubmapinventory, hubmapinventory_inventory


class PublishedBackupPlugin(ExportAndBackupPlugin):
    description = "PublishedBackupPlugin should run for datasets with the status 'published'"

    def run_plugin(self):
        return "PublishedBackupPlugin ran successfully"

    ## Future functionality
    # Back-up published datasets to appropriate location (S3)
    # Also stage for inclusion in 6-month Glacier backup?
    # Not sure how datasets are updated post-publication; that is likely a separate process--
    # maybe a DAG that people run manually when they update data


class PublishedExportPlugin(ExportAndBackupPlugin):
    description = "PublishedExportPlugin should run for datasets with the status 'published'"

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.token = get_auth_tok(**self.kwargs)

    # Should I add this to utils instead, so it can be reused?
    def get_hubmap_id(self, uuid):
        method = 'GET'
        headers = {
            'authorization': f'Bearer {self.token}',
            'content-type': 'application/json',
            'X-Hubmap-Application': 'ingest-pipeline',
            }
        http_hook = HttpHook(method, http_conn_id='ingest_api_connection')

        endpoint = f'entities/{uuid}'

        try:
            response = http_hook.run(endpoint,
                                    headers=headers,
                                    extra_options={'check_response': False})
            response.raise_for_status()
            return response.json()
        except HTTPError as e:
            print(f'ERROR: {e}')
            if e.response.status_code == codes.unauthorized:
                raise RuntimeError('entity database authorization was rejected?')
            else:
                print('benign error')
                return {}

    def run_plugin(self):
        hubmap_id = self.get_hubmap_id(self.kwargs["uuid"])["hubmap_id"]
        dbgap_study_id = self.kwargs.get("dbgap_study_id", None)
        # instance will need to be changed
        try:
            hubmapinventory_inventory.create(
                hubmap_id,
                token=self.token,
                ncores=10,
                compute_uuids=False,
                dbgap_study_id=dbgap_study_id,
                recompute_file_extension=False,
                backup=False,
                debug=True,
            )
            return "PublishedExportPlugin ran successfully"
        except Exception as e:
            formatted_exception = "".join(traceback.TracebackException.from_exception(e).format())
            return f"PublishedExportPlugin failed! Error: {e}: {formatted_exception}"

    # Need to keep track of failure: is pipeline failure notification sufficient?
