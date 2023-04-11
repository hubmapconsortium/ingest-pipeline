"""
This plugin uses regex to search for IllegalArgument errors in session.log
"""

import re

from diagnostics.diagnostic_plugin import DiagnosticResult, RegexDiagnosticPlugin

"""
DOCKER_REGEX locates the section of session.log where both docker containers are started
and captures everything after that, inclusive of the container start log, as a rough
way of logging an error taking place in this type of docker container
"""
# DOCKER_REGEX = r"job collect_ometiff_files.*docker.*job convert_to_pyramid.*docker*.*"
ILLEGAL_ARGUMENT_REGEX = r"IllegalArgument"


class IllegalArgumentDiagnosticPlugin(RegexDiagnosticPlugin):
    description = "Detects java IllegalArgument errors, within or outside of a docker container"
    order_of_application = 1.0

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.errors = []

    def do_regex_search(self, regex, line, error_message):
        match = regex.search(line)
        if match:
            self.errors.append(error_message)
            self.errors.append(line)

    def diagnose(self):
        regex = re.compile(ILLEGAL_ARGUMENT_REGEX)
        docker_match = self.get_named_docker_section("convert_to_pyramid")
        if docker_match:
            for line in docker_match.group(0).splitlines():
                self.do_regex_search(
                    regex, line, "IllegalArgument error found in docker container"
                )
        else:
            for line in open(self.session_log_path):
                self.do_regex_search(
                    regex, line, "IllegalArgument error found; no docker container found"
                )
        return DiagnosticResult(self.errors)
