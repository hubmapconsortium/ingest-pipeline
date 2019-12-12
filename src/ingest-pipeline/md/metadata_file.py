#! /usr/bin/env python

class MetadataFile(object):
    """A tool for extracting metadata from a specific file type"""
    category_name = 'Base';

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
        return {}
