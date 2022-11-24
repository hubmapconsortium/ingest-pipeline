"""
This test always passes.  It exists for development purposes.
"""

from diagnostics.diagnostic_plugin import (
    DiagnosticPlugin, DiagnosticResult, DiagnosticError
)

class AlwaysPassesDiagnosticResult(DiagnosticResult):
    def pass_fail(self):
        return True  # passes

    def to_strings(self):
        return ["nothing to see here"]

class AlwaysPassesDiagnosticPlugin(DiagnosticPlugin):
    description = "This test always passes"
    order_of_application = 1.0

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def diagnose(self):
        return AlwaysPassesDiagnosticResult([])
