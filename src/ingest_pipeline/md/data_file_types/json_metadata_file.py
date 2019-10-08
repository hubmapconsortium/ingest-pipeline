#! /usr/bin/env python

from metadata_file import MetadataFile
import json

class JSONMetadataFile(MetadataFile):
    """A metadata file type for JSON files"""
    category_name = 'JSON';

    def __init__(self, path):
        """
        path is the top level directory of the collection
        """
        self.path = path
    
    def __str__(self):
        return '<%s MetadataFile>' % self.category_name

    def __repr__(self):
        return '<%s(%s)>' % (type(self).__name__, self.path)

    def collect_metadata(self):
        print('parsing json from %s' % self.path)
        with open(self.path, 'rU') as f:
            md = json.load(f)
        return md
