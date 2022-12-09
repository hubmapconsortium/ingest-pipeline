from export_and_backup.export_and_backup_plugin import ExportAndBackupPlugin


class CodexPlugin(ExportAndBackupPlugin):
    description = "This test should run for datasets of type celldive_deepdive"

    data_type = "codex"

    def run_plugin(self):
        print("codex plugin ran successfully")
        return "codex plugin ran successfully"
