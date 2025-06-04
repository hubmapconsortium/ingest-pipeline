import os
import sys
from importlib import import_module

sys.path.append(os.path.join(os.path.dirname(__file__), "ingest-validation-tools", "src"))
sys.path.append(os.path.join(os.path.dirname(__file__), "ingest-validation-tests", "src"))
sys.path.append(os.path.join(os.path.dirname(__file__), "hubmap-inventory"))

ingest_validation_tools_upload = import_module("ingest_validation_tools.upload")
ingest_validation_tools_error_report = import_module("ingest_validation_tools.error_report")
ingest_validation_tools_validation_utils = import_module(
    "ingest_validation_tools.validation_utils"
)
ingest_validation_tools_plugin_validator = import_module(
    "ingest_validation_tools.plugin_validator"
)
ingest_validation_tests = import_module("ingest_validation_tests")
hubmapinventory = import_module("hubmapinventory")

__all__ = [
    "ingest_validation_tools_validation_utils",
    "ingest_validation_tools_upload",
    "ingest_validation_tools_error_report",
    "ingest_validation_tools_plugin_validator",
    "ingest_validation_tests",
    "hubmapinventory",
]

sys.path.pop()
sys.path.pop()
sys.path.pop()
