"""
Draft/partial plugin infrastructure for published datasets
"""
from export_and_backup.export_and_backup_plugin import ExportAndBackupPlugin
from py-hubmapbags.hubmapbags.magic import do_it


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

    def run_plugin(self):
        do_it()
        return "PublishedExportPlugin ran successfully"

    # Bringing in Ivan's code: list as a submodule OR Ivan could make it pip installable
    ## Wait this might need to take in a HuBMAP ID
    # Does this run on a UUID or dataset ID?
    # Need to keep track of failure--can't run it again if it generated UUID
    ## Testing: just pass it a DEV HuBMAP ID/UUID--make sure the code is invoking dev appropriately FIRST
