import traceback

from export_and_backup.export_and_backup_plugin import ExportAndBackupPlugin, add_path
from status_change.status_utils import get_hubmap_id_from_uuid
from utils import get_auth_tok

from airflow.configuration import conf as airflow_conf

with add_path(airflow_conf.as_dict()["connections"]["SRC_PATH"].strip("'").strip('"')):
    from submodules import hubmapinventory


class PublishedBackupPlugin(ExportAndBackupPlugin):
    description = "PublishedBackupPlugin should run for datasets with the status 'published'"

    def run_plugin(self):
        return "PublishedBackupPlugin ran successfully"

    # Future functionality
    # Back-up published datasets to appropriate location (S3)
    # Also stage for inclusion in 6-month Glacier backup?
    # Not sure how datasets are updated post-publication; that is likely a separate process--
    # maybe a DAG that people run manually when they update data


class PublishedExportPlugin(ExportAndBackupPlugin):
    description = "PublishedExportPlugin should run for datasets with the status 'published'"

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.token = get_auth_tok(**self.kwargs)

    def run_plugin(self):
        hubmap_id = get_hubmap_id_from_uuid(self.token, self.kwargs["uuid"])
        dbgap_study_id = self.kwargs.get("dbgap_study_id", None)
        try:
            hubmapinventory.inventory.create(
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
