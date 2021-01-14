#! /usr/bin/env python

import csv
import os
from pathlib import Path
from .tsv_metadata_file import TSVMetadataFile
from type_base import MetadataError
from submodules import ingest_validation_tools_submission, ingest_validation_tools_error_report

class MetadataTSVMetadataFile(TSVMetadataFile):
    """
    A metadata file type for the specialized metadata.tsv files used to store submission info
    """
    category_name = 'METADATATSV';

    def collect_metadata(self):
        print('validating {} as metadata.tsv'.format(self.path))
        dirpath = Path(os.path.dirname(self.path))
        ignore_globs = [os.path.basename(self.path), 'extras']
        submission = ingest_validation_tools_submission.Submission(directory_path=dirpath,
                                                                   dataset_ignore_globs=ignore_globs,
                                                                   submission_ignore_globs='*')
        report = ingest_validation_tools_error_report.ErrorReport(submission.get_errors())
        if report.errors:
            # Scan reports an error result
            with open('ingest_validation_tools_report.txt', 'w') as f:
                f.write(report.as_text())
            raise MetadataError('{} failed ingest validation test'.format(self.path))
        print('parsing metadatatsv from {}'.format(self.path))
        md = super().collect_metadata()

        return md
