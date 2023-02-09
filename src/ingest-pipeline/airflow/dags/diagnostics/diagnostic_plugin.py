import inspect
import sys
from importlib import util
from pathlib import Path
from typing import Iterator, List, Tuple, Union

PathOrStr = Union[str, Path]

KeyValuePair = Tuple[str, str]


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


class DiagnosticError(Exception):
    pass


class DiagnosticResult:
    def __init__(self, string_list: List[str], **kwargs):
        self.string_list = string_list

    def problem_found(self) -> bool:
        """
        a False return value means that the test was passed and the
        diagnostic did not detect a problem.
        """
        return len(self.string_list) != 0

    def to_strings(self) -> List[str]:
        return self.string_list


class DiagnosticPlugin:
    description = "This is a human-readable description"
    """str: human-readable description of the thing this plugin tests
    """

    order_of_application = 1.0
    """
    float: A floating point value which determines the order in which tests are
    applied.  Low values are carried out sooner.
    """

    def __init__(self, **kwargs):
        """
        The general context is provided by keyword arguments. The
        plugin is expected to test for the presence of the values
        it needs and raise DiagnosticError if the necessary information
        is not available.
        """
        if "data_types" not in kwargs:
            raise DiagnosticError("data_types info was not provided to constructor")
        self.data_types = kwargs["data_types"]

    def diagnose(self) -> DiagnosticResult:
        """
        Carries out the diagnostic test and returns a result.  "No Error" is
        returned by returning a DiagnosticResult instance the problem_found()
        of which returns False.
        """
        raise NotImplementedError()


class RegexDiagnosticPlugin(DiagnosticPlugin):
    def __init__(self, **kwargs):
        self.dir_path = Path(kwargs["local_directory_full_path"])

    @property
    def get_session_log_path(self):
        session_log_path = self.dir_path / "session.log"
        assert session_log_path.exists(), "session.log is not in the dataset directory"
        return session_log_path


def diagnostic_result_iter(plugin_dir: PathOrStr, **kwargs) -> Iterator[DiagnosticResult]:
    """
    Given a path to a directory of DiagnosticPlugins, iterate over the results of
    applying all the plugins to the given kwargs.

    plugin_dir: path to a directory containing classes derived from DiagnosticPlugin

    returns an iterator the values of which are DiagnosticResult instances
    """
    plugin_dir = Path(plugin_dir)
    plugins = list(plugin_dir.glob("*.py"))
    if not plugins:
        raise DiagnosticError(f"{plugin_dir}/*.py does not match any diagnostic plugins")

    sort_me = []
    with add_path(str(plugin_dir)):
        for fpath in plugin_dir.glob("*.py"):
            mod_nm = fpath.stem
            if mod_nm in sys.modules:
                mod = sys.modules[mod_nm]
            else:
                spec = util.spec_from_file_location(mod_nm, fpath)
                if spec is None:
                    raise DiagnosticError(f"bad plugin diagnostic {fpath}")
                mod = util.module_from_spec(spec)
                sys.modules[mod_nm] = mod
                spec.loader.exec_module(mod)  # type: ignore
            for _, obj in inspect.getmembers(mod):
                if (
                    inspect.isclass(obj)
                    and obj != DiagnosticPlugin
                    and obj != RegexDiagnosticPlugin
                    and issubclass(obj, DiagnosticPlugin)
                ):
                    sort_me.append((obj.order_of_application, obj.description, obj))
        sort_me.sort()
        for _, _, cls in sort_me:
            diagnostic_plugin = cls(**kwargs)
            yield diagnostic_plugin
