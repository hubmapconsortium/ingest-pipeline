#! /usr/bin/env python

import os
import json

from data_collection import DataCollection

class IMSDataCollection(DataCollection):
    category_name = 'IMS';
    
    @classmethod
    def test_match(cls, path):
        """
        Does the given path point to the top directory of a directory tree
        containing data of this collection type?
        """
        return True
    
    def __init__(self, path):
        """
        path is the top level directory of the collection
        """
        super().__init__(path)
    
    def collect_metadata(self):
        return {'hello': 'world', 'i_am_a':'ims'}
