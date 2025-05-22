import sys
import os
from pathlib import Path

import unittest

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
    python tests/pytest_runner.py --test="tests/test_fastq_validator_logic.py::TestFASTQValidatorLogic::test_fastq_groups_good" --pdb
    """
    if len(sys.argv) < 1 or "--help" in sys.argv:
        sys.exit(f"usage: {sys.argv[0]} <optional args>")
    #
    # The tricky issue here is that there is an 'airflow' module in the
    # environment, with different contents than the src/ingest-pipeline/airflow
    # directory.  We have to avoid trying to locate anything below the
    # module name "airflow" or we will mix up the two.
    #
    dags_code_path = (Path(__file__).resolve().parent.parent
                      / "src" / "ingest-pipeline" / "airflow" / "dags")
    with add_path(str(dags_code_path)):
        args = ["discover", "tests", "-vv"]
        for arg in sys.argv[1:]:
            if arg.startswith("--test="):
                args.append(arg.replace("--test=", ""))
            else:
                args.append(arg)
        loader = unittest.TestLoader()
        sys.exit(unittest.TextTestRunner().run(loader.discover(Path(__file__).parent)))


if __name__ == "__main__":
    main()
