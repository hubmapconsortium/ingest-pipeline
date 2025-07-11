import logging
import sys
import unittest
from pathlib import Path


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


def main():
    """
    Receives args from test.sh but can also accept pytest flags or
    custom --test= arg directly, e.g.
    python tests/unittest_runner.py --test="test_status_changer.TestEntityUpdater.test_fields_to_change"
    """
    if len(sys.argv) < 1 or "--help" in sys.argv:
        sys.exit(f"usage: {sys.argv[0]} <optional args>")
    #
    # The tricky issue here is that there is an 'airflow' module in the
    # environment, with different contents than the src/ingest-pipeline/airflow
    # directory.  We have to avoid trying to locate anything below the
    # module name "airflow" or we will mix up the two.
    #
    dags_code_path = (
        Path(__file__).resolve().parent.parent / "src" / "ingest-pipeline" / "airflow" / "dags"
    )
    with add_path(str(dags_code_path)):
        kwargs = {"verbosity": 2}
        test_names = []
        for arg in sys.argv[1:]:
            if arg.startswith("--test="):
                test_names.append(arg.replace("--test=", ""))
            elif arg == "-v":
                kwargs["verbosity"] += 1
            else:
                logging.warn(f"Unmapped arg {arg}")
        loader = unittest.TestLoader()
        runner = unittest.TextTestRunner(**kwargs)
        if test_names:
            tests = loader.loadTestsFromNames(test_names)
        else:
            tests = loader.discover(Path(__file__).parent)
        rslt = runner.run(tests)
        sys.exit(len(rslt.errors) + len(rslt.failures))


if __name__ == "__main__":
    main()
