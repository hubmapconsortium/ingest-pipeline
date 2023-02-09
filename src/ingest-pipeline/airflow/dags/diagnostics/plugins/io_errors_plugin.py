"""
This plugin uses regex to search for IO errors in session.log
"""

import re

from diagnostics.diagnostic_plugin import DiagnosticResult, RegexDiagnosticPlugin

IO_ERROR_REGEX = r"(?=.*(OSError))(?=.*(Input\/output error))"


class IOErrorsDiagnosticPlugin(RegexDiagnosticPlugin):
    description = "Detects input/output OSErrors"
    order_of_application = 1.0

    def diagnose(self):
        regex = re.compile(IO_ERROR_REGEX)
        errors = []
        for line in open(self.get_session_log_path):
            match = regex.search(line)
            if match:
                errors.append("Input/output error found!")
                errors.append(line)
        return DiagnosticResult(errors)
