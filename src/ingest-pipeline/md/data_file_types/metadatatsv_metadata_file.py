#! /usr/bin/env python

import csv
import os
from pathlib import Path
from .tsv_metadata_file import TSVMetadataFile
from type_base import MetadataError
from submodules import (ingest_validation_tools_upload,
                        ingest_validation_tools_error_report,
                        ingest_validation_tests)


# If True, this will cause the ingest-validation-tools validation mechanism to
# be applied to the metadata.tsv file.  If false, validation is not applied,
# which makes this MetadataFile functionally identical to its parent.
ENABLE_INGEST_VALIDATION = False


class MetadataTSVMetadataFile(TSVMetadataFile):
    """
    A metadata file type for the specialized metadata.tsv files used to store upload and submission info
    """
    category_name = 'METADATATSV';

    def collect_metadata(self):
        if ENABLE_INGEST_VALIDATION:
            print('validating {} as metadata.tsv'.format(self.path))
            dirpath = Path(os.path.dirname(self.path))
            ignore_globs = [os.path.basename(self.path), 'extras',
                            'validation_report.txt']
            plugin_path = [path for path in ingest_validation_tests.__path__][0]
            #
            # Uncomment offline=True below to avoid validating orcid_id URLs &etc
            #
            upload = ingest_validation_tools_upload.Upload(
                directory_path=dirpath,
                dataset_ignore_globs=ignore_globs,
                upload_ignore_globs='*',
                #offline=True,
                add_notes=False
            )
            if upload.get_errors():
                # Scan reports an error result
                report = ingest_validation_tools_error_report.ErrorReport(upload.get_errors())
                with open('ingest_validation_tools_report.txt', 'w') as f:
                    f.write(report.as_text())
                raise MetadataError(f'{self.path} failed ingest validation test')
        print('parsing metadatatsv from {}'.format(self.path))
        md = super().collect_metadata()

        return md
