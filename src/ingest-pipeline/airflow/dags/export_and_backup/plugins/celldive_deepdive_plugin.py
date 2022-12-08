from export_and_backup.export_and_backup_plugin import ExportAndBackupPlugin


class CelldiveDeepdivePlugin(ExportAndBackupPlugin):
    description = "This test should run for datasets of type celldive_deepdive"

    data_type = "celldive_deepdive"

    def run_plugin(self):
        return "celldive_deepdive plugin ran successfully"
