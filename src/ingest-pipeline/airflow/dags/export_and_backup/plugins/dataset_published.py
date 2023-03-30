from export_and_backup.export_and_backup_plugin import ExportAndBackupPlugin, add_path

from airflow.configuration import conf as airflow_conf

with add_path(airflow_conf.as_dict()["connections"]["SRC_PATH"].strip("'").strip('"')):
    from submodules import hubmapbags_magic


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

    def run_plugin(self):
        hubmap_id = self.kwargs["hubmap_id"]
        dbgap_study_id = self.kwargs.get("dbgap_study_id", None)
        token = self.kwargs["token"]
        # instance will need to be changed
        try:
            hubmapbags_magic.do_it(
                hubmap_id,
                dbgap_study_id=dbgap_study_id,
                compute_uuids=False,
                overwrite=False,
                copy_output_to=None,
                build_bags=False,
                token=token,
                instance="dev",
                debug=True,
            )
            return "PublishedExportPlugin ran successfully"
        except Exception as e:
            return f"PublishedExportPlugin failed! Error: {e}"

    # Need to keep track of failure
