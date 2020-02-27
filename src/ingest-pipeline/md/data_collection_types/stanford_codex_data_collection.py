#! /usr/bin/env python

import os
import json
import glob

from type_base import MetadataError
from data_collection import DataCollection
from .akoya_codex_data_collection import AkoyaCODEXDataCollection

class StanfordCODEXDataCollection(AkoyaCODEXDataCollection):
    category_name = 'STANFORD_CODEX';
    top_target = 'Experiment.json'

    # expected_file pairs are (globable name, filetype key)
    expected_files = [('processingOptions.json', "JSON"),
                      ('Experiment.json', "JSON"),
                      ('channelNames.txt', "TXTWORDLIST"),
                      ]
    
    optional_files = []
    
#     @classmethod
#     def test_match(cls, path):
#         """
#         Does the given path point to the top directory of a directory tree
#         containing data of this collection type?
#         """
#         for match, _ in cls.expected_files:
#             print('testing %s' % match)
#             if not any(glob.iglob(os.path.join(path,match))):
#                 print('not found!')
#                 return False
#         return True
#     
#     def __init__(self, path):
#         """
#         path is the top level directory of the collection
#         """
#         super().__init__(path)
    
    def collect_metadata(self):
        rslt = super(StanfordCODEXDataCollection, self).collect_metadata()
        # The superclass will have looked in the wrong place for components
        cl = []
        for fname in os.listdir(os.path.join(self.topdir, self.offsetdir)):
            fullname = os.path.join(self.topdir, self.offsetdir, fname)
            if os.path.isdir(fullname) and fname.startswith('Cyc'):
                cl.append(fname)
        rslt['components'] = cl
        hande_cl = []
        for fname in os.listdir(os.path.join(self.topdir, self.offsetdir)):
            fullname = os.path.join(self.topdir, self.offsetdir, fname)
            if os.path.isdir(fullname) and fname.startswith('HandE_'):
                hande_cl.append(fname)
        rslt['hande_components'] = hande_cl
        rslt['collectiontype'] = 'codex'
        
        return rslt
    
    def filter_metadata(self, metadata):
        """
        This extracts the metadata which is actually desired downstream from the bulk of the
        metadata which has been collected.
        
        """
        rslt = {'collectiontype': metadata['collectiontype'],
                'components': metadata['components']
        }
#         for elt in metadata:
#             # each element is the pathname of the file from which it was extracted
#             if not os.path.dirname(elt) and elt.endswith('spatial_meta.txt'):
#                 spatial_meta = metadata[elt]
#                 break
#             else:
#                 raise MetadataError('The spatial metadata is unexpectedly missing')
# 
#         rslt['ccf_spatial'] = {k : v for k, v in spatial_meta.items()}
        
        rslt['other_meta'] = metadata.copy()  # for debugging
        return rslt
            
        
