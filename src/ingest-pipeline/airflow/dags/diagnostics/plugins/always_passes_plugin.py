"""
This test always passes.  It exists for development purposes.
"""

from diagnostic_plugin import DiagnosticPlugin, DiagnosticResult, DiagnosticError

class AlwaysPassesDiagnosticResult(DiagnosticResult):
    def pass_fail(self):
        return True  # passes

    def to_strings(self):
        return ["nothing to see here"]

class AlwaysPassesDiagnosticPlugin(DiagnosticPlugin):
    def __init__(self, **kwargs):
        super.__init__(**kwargs)

    def diagnose(self):
        return AlwaysPassesDiagnosticResult([])
