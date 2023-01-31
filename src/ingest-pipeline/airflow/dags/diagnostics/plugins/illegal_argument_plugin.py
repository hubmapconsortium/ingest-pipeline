"""
This plugin uses regex to search for IllegalArgument errors in session.log
"""

import re
from itertools import dropwhile, groupby

from diagnostics.diagnostic_plugin import DiagnosticResult, RegexDiagnosticPlugin

"""
DOCKER_REGEX locates the section of session.log where both docker containers are started
and captures everything after that, inclusive of the container start log, as a rough
way of logging an error taking place in this type of docker container
"""
DOCKER_REGEX = r"job collect_ometiff_files.*docker.*job convert_to_pyramid.*docker*.*"
ILLEGAL_ARGUMENT_REGEX = r"IllegalArgument"


class IllegalArgumentDiagnosticPlugin(RegexDiagnosticPlugin):
    description = "Detects java IllegalArgument errors, within or outside of a docker container"
    order_of_application = 1.0

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.errors = []

    # This feels a little overdone, it's possible this could be done with a groupby?
    def get_docker_section(self):
        session_log_data = open(self.get_session_log_path).read()
        docker_regex = re.compile(DOCKER_REGEX, re.DOTALL)
        docker_match = docker_regex.search(session_log_data)
        if not docker_match:
            return None
        return docker_match

    def do_regex_search(self, regex, line, error_message):
        match = regex.search(line)
        if match:
            self.errors.append(error_message)
            self.errors.append(line)

    def diagnose(self):
        regex = re.compile(ILLEGAL_ARGUMENT_REGEX)
        docker_match = self.get_docker_section()
        if docker_match:
            for line in docker_match.group(0).splitlines():
                self.do_regex_search(
                    regex, line, "IllegalArgument error found in docker container"
                )
        else:
            for line in open(self.get_session_log_path):
                self.do_regex_search(
                    regex, line, "IllegalArgument error found; no docker container found"
                )
        return DiagnosticResult(self.errors)
