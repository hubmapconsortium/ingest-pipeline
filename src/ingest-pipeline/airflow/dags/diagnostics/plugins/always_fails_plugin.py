"""
This test always fails.  It exists for development purposes.
"""

from diagnostics.diagnostic_plugin import (
    DiagnosticPlugin, DiagnosticResult, DiagnosticError
)

class AlwaysFailsDiagnosticResult(DiagnosticResult):
    def pass_fail(self):
        return False  # passes

    def to_strings(self):
        return ["This test failed because it always fails"]

class AlwaysFailsDiagnosticPlugin(DiagnosticPlugin):
    description = "This test always fails"
    order_of_application = 2.0

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def diagnose(self):
        return AlwaysFailsDiagnosticResult([])
