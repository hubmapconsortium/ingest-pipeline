import re
from collections import defaultdict
from functools import cached_property
from typing import Dict

from yaml import dump


class ErrorClassifier:
    def __init__(self, errors):
        self.errors = errors

    def _local_validation_get_error_type_and_path(self, value: str):
        """
        Pull out path with regex patterns, return path and error type
        """
        regex_patterns = {
            "Validation error": r".*(?=\ \(as.*\))",
            "Error opening TSV": r".*(?=, column)",
        }
        for error_type, pattern in regex_patterns.items():
            regex = re.compile(pattern)
            match = regex.match(value)
            if match:
                return error_type, match[0]
        return None, None

    def classify_local_validation_errors(self, local_validation_section: Dict) -> Dict[str, list]:
        classified_errors = defaultdict(list)
        for key in local_validation_section.keys():
            # TODO: change output so this fits in _local_validation_get_error_type_and_path?
            if key == "Schema version is deprecated":
                pass
            error_type, path = self._local_validation_get_error_type_and_path(key)
            if path and error_type:
                classified_errors["path"].append(error_type)
        return classified_errors

    def classify_api_validation_errors(self, api_validation_section: Dict) -> Dict[str, list]:
        classified_errors = defaultdict(list)
        for key, value in api_validation_section.items():
            for error_type in value.keys():
                classified_errors[key].append(error_type)
        return classified_errors

    def classify_ref_errors(self, ref_validation_section: Dict) -> Dict[str, list]:
        classified_errors = {}
        for key, value in ref_validation_section.items():
            if key == "No References":
                classified_errors.update(
                    {file: ["Not referenced by any TSVs"] for file in value.get("Files", [])}
                )
                classified_errors.update(
                    {dir: ["Not referenced by any TSVs"] for dir in value.get("Directories", [])}
                )
            if key == "Multiple References":
                classified_errors.update({path: ["Referenced by multiple TSVs"] for path in value})
        return classified_errors

    @cached_property
    def normalized_errors(self):
        """
        Preflight, Local Validation Errors, CEDAR Validation Errors, Directory Errors, Reference Errors, Data Errors
        """
        normalized_errors = {}
        local_validation = {}
        api_validation = {}
        ref_validation = {}
        if self.errors.get("Preflight"):
            normalized_errors["Preflight"] = [
                f"{key}: {value}" for key, value in self.errors.get("Preflight").items()
            ]
        if self.errors.get("Upload Errors"):
            upload_section = self.errors.get("Upload Errors")
            if type(upload_section) is dict:
                tsv_error_section = upload_section.get("TSV Errors")
                # TODO: Account for "file is missing data_path or contributors_path"
                if type(tsv_error_section) is dict:
                    for tsv_errors in tsv_error_section.values():
                        if tsv_errors.get("Local Validation Errors"):
                            local_validation.update(
                                self.classify_local_validation_errors(
                                    tsv_errors.get("Local Validation Errors", {})
                                )
                            )
                        if tsv_errors.get("CEDAR Validation Errors"):
                            api_validation.update(
                                self.classify_api_validation_errors(
                                    tsv_errors.get("CEDAR Validation Errors", {})
                                )
                            )
                if upload_section.get("Directory Errors"):
                    # TODO: classify
                    normalized_errors.update(
                        {"Upload Directory Errors": upload_section.get("Directory Errors")}
                    )
        if self.errors.get("Metadata TSV Validation Errors"):
            validation_section = self.errors.get("Metadata TSV Validation Errors")
            if type(validation_section) is dict:
                if validation_section.get("Local Validation Errors"):
                    local_validation.update(
                        self.classify_local_validation_errors(
                            validation_section.get("Local Validation Errors", {})
                        )
                    )
                if validation_section.get("CEDAR Validation Errors"):
                    api_validation.update(
                        self.classify_api_validation_errors(
                            validation_section.get("CEDAR Validation Errors", {})
                        )
                    )
        if self.errors.get("Reference Errors"):
            ref_section = self.errors.get("Reference Errors")
            if type(ref_section) is dict:
                normalized_errors.update(
                    {"Reference Errors": self.classify_ref_errors(ref_section)}
                )
        normalized_errors["Local Validation Errors"] = local_validation | ref_validation
        normalized_errors["API Validation Errors"] = api_validation
        if self.errors.get("Plugin Errors"):
            plugin_section = self.errors.get("Plugin Errors")
            if type(plugin_section) is dict:
                normalized_errors["Data Errors"] = [key for key in plugin_section.keys()]
        return normalized_errors

    @cached_property
    def consolidated_paths(self):
        consolidated_paths = defaultdict(list)
        for error_type, report in self.normalized_errors.items():
            if type(report) is dict:
                for path, errors in report.items():
                    if error_type == "API Validation Errors" and "Validation Errors" in errors:
                        errors.remove("Validation Errors")
                        errors.append("Spreadsheet Validator Errors")
                    consolidated_paths[path].extend(errors)
        return dict(consolidated_paths)

    @cached_property
    def consolidated_types(self):
        consolidated_types = defaultdict(list)
        for error_type, report in self.normalized_errors.items():
            if type(report) is dict:
                for path, errors in report.items():
                    if error_type == "API Validation Errors" and "Validation Errors" in errors:
                        errors.remove("Validation Errors")
                        errors.append("Spreadsheet Validator Errors")
                    for error_str in errors:
                        consolidated_types[error_str].append(path)
        return dict(consolidated_types)

    def report_errors_by_path(self) -> str:
        msg = ""
        for path, errors in self.consolidated_paths.items():
            msg += f"{path} has the following errors: {', '.join([error for error in errors])}"
        data_errors = self.normalized_errors.get("Data Errors")
        if data_errors and type(data_errors) is list:
            msg += f"Data errors: {', '.join([error for error in data_errors])}"
        return msg

    def report_errors_by_type(self) -> str:
        msg = ""
        for error_type, paths in self.consolidated_types.items():
            msg += f"{error_type}: {', '.join([path for path in paths])}"
        data_errors = self.normalized_errors.get("Data Errors")
        if data_errors and type(data_errors) is list:
            msg += f"Data errors: {', '.join([error for error in data_errors])}"
        return msg

    def report_errors_basic(self) -> str:
        unique_errors = set()
        for errors in [
            *self.consolidated_paths.values(),
            *self.normalized_errors.get("Data Errors", []),
        ]:
            unique_errors.update({error for error in errors})
        str_errors = ", ".join([error for error in unique_errors])
        return str_errors

    def consolidated_types_yaml(self):
        combined_errors = self.consolidated_types | self.normalized_errors.get("Data Errors", {})
        return dump(combined_errors, sort_keys=False)

    def consolidated_paths_yaml(self):
        combined_errors = self.consolidated_paths | self.normalized_errors.get("Data Errors", {})
        return dump(combined_errors, sort_keys=False)
