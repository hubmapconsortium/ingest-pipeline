"""
This test iterates through the plugins in ingest-validation-tests,
applying those with costs <= a fixed threshold to the parent of
the dataset being diagnosed and reporting an error if a validation
test fails.
"""

from pathlib import Path

from diagnostics.diagnostic_plugin import (
    DiagnosticPlugin, DiagnosticResult, DiagnosticError, add_path
)

from airflow.configuration import conf as airflow_conf

with add_path(airflow_conf.as_dict()['connections']['SRC_PATH'].strip("'").strip('"')):
    from submodules import ingest_validation_tools_plugin_validator as plugin_validator
    from submodules import ingest_validation_tests


# Apply no validation plugins with costs greater than this
PLUGIN_COST_THRESHOLD = 1.0

class ValidationPluginDiagnosticPlugin(DiagnosticPlugin):
    description = "Apply some ingest-validation-tests to parent datasets"
    order_of_application = 1.9

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.parent_dataset_uuid_list = kwargs['parent_dataset_uuid_list']
        self.parent_dataset_full_path_list = kwargs['parent_dataset_full_path_list']
        self.parent_dataset_data_types_list = kwargs['parent_dataset_data_types_list']
        self.parent_dataset_data_path_list = kwargs['parent_dataset_data_path_list']

    def diagnose(self):
        string_list = []
        vp_path = ingest_validation_tests.__path__._path[0]
        for cls in plugin_validator.validation_class_iter(vp_path):
            if cls.cost <= PLUGIN_COST_THRESHOLD:
                for parent_uuid, parent_path, data_types, parent_data_path in zip(
                        self.parent_dataset_uuid_list,
                        self.parent_dataset_full_path_list,
                        self.parent_dataset_data_types_list,
                        self.parent_dataset_data_path_list
                ):
                    if len(data_types) != 1:
                        raise DiagnosticError(f"Expected one assay type for {parent_uuid}"
                                              f", found {data_types}")
                    if parent_data_path is None:
                        raise DiagnosticError(f"Parent dataset {parent_uuid} has no data_path")
                    validator = cls(Path(parent_path) / parent_data_path,
                                    data_types[0])
                    err_strings = [err for err in validator.collect_errors()]
                    # Need kwargs for collect_errors?
                    # Need rel path for cls constructor?
                    if err_strings:
                        string_list.append(f'Applying {cls.description} to {parent_uuid} produced:')
                        for err_s in err_strings:
                            string_list.append(err_s)

        return DiagnosticResult(string_list)
