"""
This plugin uses regex to search for errors in session.log
"""

import re
from pathlib import Path

from diagnostics.diagnostic_plugin import (
    DiagnosticPlugin,
    DiagnosticResult,
    DiagnosticError,
)


# TODO: maybe there's a way to encode case insensitivity directly into the regex string
# rather than doing it in the combine method?

# Finds all instances of the word "error"
FIND_ERROR_REGEX = r"(\S+)?error(\S+)?"
# Finds OSError and BrokenPipeError (test--TODO: needs better modularizing)
FIND_IO_ERROR_REGEX = r"(oserror)|(brokenpipeerror)"


class FindErrorsDiagnosticResult(DiagnosticResult):
    def problem_found(self):
        if len(self.string_list) > 0:
            return True
        return False

    def to_strings(self):
        print("Errors:")
        for index, string in enumerate(self.string_list):
            if index == 10:
                break
            print(string)
        return self.string_list


class FindErrorsDiagnosticPlugin(DiagnosticPlugin):
    description = "This test should locate instances of the string 'Error'"
    order_of_application = 1.0

    def __init__(self, **kwargs):
        self.dir_path = Path(kwargs["local_directory_full_path"])

    def diagnose(self):
        session_log_path = self.dir_path / "session.log"
        assert session_log_path.exists(), "session.log is not in the dataset directory"
        regex = re.compile(FIND_IO_ERROR_REGEX, re.I)
        errors = []
        for line in open(session_log_path):
            match = regex.search(line)
            if match:
                # TODO: Currently adds the entire line, could instead return just error name
                # and/or do more parsing to separate error name and contextual info
                errors.append(line)
        return FindErrorsDiagnosticResult(errors)
