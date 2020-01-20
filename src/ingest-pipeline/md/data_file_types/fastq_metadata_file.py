#! /usr/bin/env python

from metadata_file import MetadataFile

class FASTQMetadataFile(MetadataFile):
    """A metadata file type for fastq.gz files"""
    category_name = 'FASTQ';

    def collect_metadata(self):
        return {}
