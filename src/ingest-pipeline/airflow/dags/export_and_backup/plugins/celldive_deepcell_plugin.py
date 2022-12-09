from export_and_backup.export_and_backup_plugin import ExportAndBackupPlugin


class CelldiveDeepcellPlugin(ExportAndBackupPlugin):
    description = "This test should run for datasets of type celldive_deepcell"

    data_type = "celldive_deepcell"

    def run_plugin(self):
        return "celldive_deepcell plugin ran successfully"
