from export_and_backup.export_and_backup_plugin import ExportAndBackupPlugin


class ErrorExportPlugin(ExportAndBackupPlugin):
    description = "ErrorExportPlugin should run for datasets with the status 'error'"

    def run_plugin(self):
        return "ErrorExportPlugin ran successfully"
