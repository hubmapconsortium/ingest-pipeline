#! /usr/bin/env python

import os
import json


class DataCollection(object):
    category_name = 'Base';
    
    @classmethod
    def test_match(cls, path):
        """
        Does the given path point to the top directory of a directory tree
        containing data of this collection type?
        """
        return False
    
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
