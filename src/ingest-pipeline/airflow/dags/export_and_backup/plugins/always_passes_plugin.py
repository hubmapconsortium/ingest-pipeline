from export_and_backup.export_and_backup_plugin import ExportAndBackupPlugin


class AlwaysPassesPlugin(ExportAndBackupPlugin):
    def run_plugin(self):
        return "I passed!"
