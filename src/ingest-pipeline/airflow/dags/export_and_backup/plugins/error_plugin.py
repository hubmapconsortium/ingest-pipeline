from export_and_backup.export_and_backup_plugin import ExportAndBackupPlugin


class ErrorPlugin(ExportAndBackupPlugin):
    description = "This set of plugins should run for datasets with the status 'error'"

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def error_export_plugin(self):
        return "error_export_plugin ran successfully"
