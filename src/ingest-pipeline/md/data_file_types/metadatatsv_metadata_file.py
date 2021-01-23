#! /usr/bin/env python

import csv
import os
from pathlib import Path
from .tsv_metadata_file import TSVMetadataFile
from type_base import MetadataError
from submodules import (ingest_validation_tools_submission,
                        ingest_validation_tools_error_report,
                        ingest_validation_tests)

class MetadataTSVMetadataFile(TSVMetadataFile):
    """
    A metadata file type for the specialized metadata.tsv files used to store submission info
    """
    category_name = 'METADATATSV';

    def collect_metadata(self):
        print('validating {} as metadata.tsv'.format(self.path))
        dirpath = Path(os.path.dirname(self.path))
        ignore_globs = [os.path.basename(self.path), 'extras']
        plugin_path = [path for path in ingest_validation_tests.__path__][0]
        #
        # Uncomment offline=True below to avoid validating orcid_id URLs &etc
        #
        submission = ingest_validation_tools_submission.Submission(directory_path=dirpath,
                                                                   dataset_ignore_globs=ignore_globs,
                                                                   submission_ignore_globs='*',
                                                                   plugin_path=plugin_path,
                                                                   #offline=True,
                                                                   add_notes=False
                                                                   )
        if submission.get_errors():
            # Scan reports an error result
            report = ingest_validation_tools_error_report.ErrorReport(submission.get_errors())
            with open('ingest_validation_tools_report.txt', 'w') as f:
                f.write(report.as_text())
            raise MetadataError('{} failed ingest validation test'.format(self.path))
        print('parsing metadatatsv from {}'.format(self.path))
        md = super().collect_metadata()

        return md
