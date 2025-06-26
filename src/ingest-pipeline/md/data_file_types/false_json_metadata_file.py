#! /usr/bin/env python

from metadata_file import MetadataFile
import json

class FalseJSONMetadataFile(MetadataFile):
    """A metadata file type for files that claim to be JSON files but aren't """
    category_name = 'FALSE_JSON'

    def collect_metadata(self):
        print('not parsing json from %s' % self.path)
        with open(self.path, 'r') as f:
            md = {}
        return md
