import sys
import inspect
from pathlib import Path
from importlib import util


class add_path:
    """
    Add an element to sys.path using a context.
    Thanks to Eugene Yarmash https://stackoverflow.com/a/39855753
    """

    def __init__(self, path):
        self.path = path

    def __enter__(self):
        sys.path.insert(0, self.path)

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            sys.path.remove(self.path)
        except ValueError:
            pass


class ExportAndBackupPlugin:
    description = "This is a human-readable description"

    order_of_application = 1.0

    data_type = None

    def __init__(self, **kwargs):
        """
        data_types may be necessary to provide to plugins
        """
        if "data_types" not in kwargs:
            raise Exception("data_types info was not provided to constructor")
        self.data_types = kwargs["data_types"]

    def run_plugin(self):
        raise NotImplementedError()


# This is shamelessly stolen from diagnostic_plugin
def export_and_backup_result_iter(plugin_dir, **kwargs):
    plugin_dir = Path(plugin_dir)
    plugins = list(plugin_dir.glob("*.py"))
    if not plugins:
        raise Exception(f"{plugin_dir}/*.py does not match any export_and_backup plugins")
    sort_me = []
    with add_path(str(plugin_dir)):
        for fpath in plugin_dir.glob("*.py"):
            mod_nm = fpath.stem
            if mod_nm in sys.modules:
                mod = sys.modules[mod_nm]
            else:
                spec = util.spec_from_file_location(mod_nm, fpath)
                if spec is None:
                    raise Exception(f"bad plugin diagnostic {fpath}")
                mod = util.module_from_spec(spec)
                sys.modules[mod_nm] = mod
                spec.loader.exec_module(mod)  # type: ignore
            for _, obj in inspect.getmembers(mod):
                if (
                    inspect.isclass(obj)
                    and obj != ExportAndBackupPlugin
                    and issubclass(obj, ExportAndBackupPlugin)
                    and inspect.getattr_static(obj, "data_type") in kwargs["data_types"]
                ):
                    sort_me.append((obj.order_of_application, obj.description, obj))
        sort_me.sort()
        for _, _, cls in sort_me:
            plugin = cls(**kwargs)
            yield plugin
