from export_and_backup.export_and_backup_plugin import ExportAndBackupPlugin
from status_change.status_utils import get_hubmap_id_from_uuid
from utils import get_auth_tok


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
        return "PublishedExportPlugin ran successfully"
