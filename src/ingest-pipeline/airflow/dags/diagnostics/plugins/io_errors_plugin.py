"""
This plugin uses regex to search for IO errors in session.log
"""

import re
from pathlib import Path

from diagnostics.diagnostic_plugin import (
    DiagnosticPlugin,
    DiagnosticResult,
)

IO_ERROR_REGEX = r"(?=.*(OSError))(?=.*(Input\/output error))"


class IOErrorsDiagnosticPlugin(DiagnosticPlugin):
    description = "Detects input/output OSErrors"
    order_of_application = 1.0

    def __init__(self, **kwargs):
        self.dir_path = Path(kwargs["local_directory_full_path"])

    def diagnose(self):
        session_log_path = self.dir_path / "session.log"
        assert session_log_path.exists(), "session.log is not in the dataset directory"
        regex = re.compile(IO_ERROR_REGEX)
        errors = []
        for line in open(session_log_path):
            match = regex.search(line)
            if match:
                errors.append(line)
        print("Input/output error found!")
        return DiagnosticResult(errors)
