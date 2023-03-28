from export_and_backup.export_and_backup_plugin import ExportAndBackupPlugin


class PublishedBackupPlugin(ExportAndBackupPlugin):
    description = "PublishedBackupPlugin should run for datasets with the status 'error'"

    def run_plugin(self):
        return "PublishedBackupPlugin ran successfully"
