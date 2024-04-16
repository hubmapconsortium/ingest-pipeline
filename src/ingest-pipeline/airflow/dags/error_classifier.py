class ErrorClassifier:
    def __init__(self, errors):
        self.errors = errors

    def classify(self):
        errors = []
        if self.errors.preflight:
            errors.append("Fatal Errors")
        if self.errors.directory:
            errors.append("Directory Errors")
        if self.errors.upload_metadata:
            errors.append("Antibodies/Contributors Errors")
        if self.errors.metadata_validation_api:
            errors.append("Spreadsheet Validator Errors")
        if self.errors.metadata_validation_local:
            errors.append("Local Validation Errors")
        if self.errors.metadata_url_errors:
            errors.append("URL Errors")
        if self.errors.reference:
            errors.append("Reference Errors")
        if self.errors.plugin:
            errors.append("Data File Errors")
        return ", ".join(errors)
