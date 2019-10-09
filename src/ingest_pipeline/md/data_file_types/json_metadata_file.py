#! /usr/bin/env python

from metadata_file import MetadataFile
import json

class JSONMetadataFile(MetadataFile):
    """A metadata file type for JSON files"""
    category_name = 'JSON';

    def collect_metadata(self):
        print('parsing json from %s' % self.path)
        with open(self.path, 'rU') as f:
            md = json.load(f)
        return md
