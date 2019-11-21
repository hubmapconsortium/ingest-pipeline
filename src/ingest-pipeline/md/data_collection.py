#! /usr/bin/env python

import os
import json

from metadata_file import MetadataFile
import data_file_types


_MD_TYPE_TBL = None  # lazy initialization


class DataCollection(object):
    category_name = 'Base';
    
    @classmethod
    def test_match(cls, path):
        """
        Does the given path point to the top directory of a directory tree
        containing data of this collection type?
        """
        return False

    def get_md_type_tbl(self):
        global _MD_TYPE_TBL

        if _MD_TYPE_TBL is None:
            tbl = {}
            for nm in dir(data_file_types):
                elt = getattr(data_file_types, nm)
                if isinstance(elt, type) and issubclass(elt, MetadataFile):
                    tbl[elt.category_name.upper()] = elt
            _MD_TYPE_TBL = tbl
        return _MD_TYPE_TBL

    def __init__(self, path):
        """
        path is the top level directory of the collection
        """
        self.topdir = path
    
    def __str__(self):
        return '<%s DataCollectionCategory>' % self.category_name

    def __repr__(self):
        return '<%s(%s)>' % (type(self).__name__, self.topdir)

    def collect_metadata(self):
        return {}
    
    def filter_metadata(self, metadata):
        return metadata.copy()
