from export_and_backup.export_and_backup_plugin import ExportAndBackupPlugin


class ErrorBackupPlugin(ExportAndBackupPlugin):
    description = "This test should run for datasets with the status 'error'"

    def run_plugin(self):
        return "error_backup plugin ran successfully"
