"""
This plugin uses regex to search for errors in session.log
"""

import re
from pathlib import Path

from diagnostics.diagnostic_plugin import (
    DiagnosticPlugin,
    DiagnosticResult,
)


# TODO: maybe there's a way to encode case insensitivity directly into the regex string
# rather than doing it in the combine method? That is, if we want case insensitivity

# Finds all instances of the word "error"
FIND_ERROR_REGEX = r"(\S+)?error(\S+)?"
# Finds OSError and BrokenPipeError (test--TODO: needs better modularizing)
FIND_OS_ERROR_REGEX = r"(oserror)|(brokenpipeerror)"


class FindErrorsDiagnosticPlugin(DiagnosticPlugin):
    description = "This test should locate instances of the string 'Error'"
    order_of_application = 1.0

    def __init__(self, **kwargs):
        self.dir_path = Path(kwargs["local_directory_full_path"])

    def diagnose(self):
        session_log_path = self.dir_path / "session.log"
        assert session_log_path.exists(), "session.log is not in the dataset directory"
        regex = re.compile(FIND_ERROR_REGEX, re.I)
        errors = []
        for line in open(session_log_path):
            match = regex.search(line)
            if match:
                # TODO: Currently adds the entire line, could instead return just error name
                # and/or do more parsing to separate error name and contextual info
                errors.append(line)
        return DiagnosticResult(errors)
