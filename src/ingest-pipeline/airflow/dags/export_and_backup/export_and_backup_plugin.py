from pathlib import Path


class ExportAndBackupPlugin:
    def run_plugin(self):
        raise NotImplementedError()


def export_and_backup_result_iter(plugin_dir: Path | str, **kwargs) -> list:
    plugin_dir = Path(plugin_dir)
    plugins = list(plugin_dir.glob("*.py"))
    if not plugins:
        raise Exception(f"{plugin_dir}/*.py does not match any export_and_backup plugins")
    return plugins
