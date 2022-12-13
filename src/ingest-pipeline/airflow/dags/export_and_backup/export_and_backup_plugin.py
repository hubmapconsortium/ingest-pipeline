import sys
import inspect
from pathlib import Path
from importlib import util
from typing import Iterator


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

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def run_plugin(self):
        raise NotImplementedError()


# This is shamelessly stolen from diagnostic_plugin
# TODO: remove print statements (testing)
def export_and_backup_result_iter(plugin_dir: Path, **kwargs) -> Iterator[ExportAndBackupPlugin]:
    plugin_dir = Path(plugin_dir)
    plugins = list(plugin_dir.glob("*.py"))
    if not plugins:
        raise Exception(f"{plugin_dir}/*.py does not match any export_and_backup plugins")
    sort_me = []
    with add_path(str(plugin_dir)):
        for fpath in plugin_dir.glob("*.py"):
            print("fpath: ", fpath)
            mod_nm = fpath.stem
            print("mod_nm: ", mod_nm)
            """
            This means that plugins listed in export_and_backup_map need to match filenames exactly,
            which seems a little fragile?
            """
            if mod_nm not in kwargs["plugins"]:
                continue
            if mod_nm in sys.modules:
                mod = sys.modules[mod_nm]
                print("mod1: ", mod)
            else:
                spec = util.spec_from_file_location(mod_nm, fpath)
                if spec is None:
                    raise Exception(f"bad plugin {fpath}")
                mod = util.module_from_spec(spec)
                print("mod2: ", mod)
                sys.modules[mod_nm] = mod
                spec.loader.exec_module(mod)  # type: ignore
            for _, obj in inspect.getmembers(mod):
                print(obj)
                print(inspect.getattr_static(obj, "data_type", None))
                print(kwargs["data_types"])
                if (
                    inspect.isclass(obj)
                    and obj != ExportAndBackupPlugin
                    and issubclass(obj, ExportAndBackupPlugin)
                ):
                    sort_me.append((obj.order_of_application, obj.description, obj))
        print("sort_me: ", sort_me)
        sort_me.sort()
        for _, _, cls in sort_me:
            plugin = cls(**kwargs)
            yield plugin
