#! /usr/bin/env python

from metadata_file import MetadataFile

class IgnoreMetadataFile(MetadataFile):
    """A metadata file type for files containing no useful metadata"""
    category_name = 'Ignore';

    def collect_metadata(self):
        print('ignoring %s' % self.path)
        return None
