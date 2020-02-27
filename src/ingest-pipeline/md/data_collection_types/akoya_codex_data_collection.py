#! /usr/bin/env python

import os
import json
import glob

from type_base import MetadataError
from data_collection import DataCollection

class AkoyaCODEXDataCollection(DataCollection):
    category_name = 'AKOYA_CODEX';
    top_target = 'experiment.json'

    # expected_file pairs are (globable name, filetype key)
    expected_files = [('experiment.json',
                       "JSON"),
                       ('segmentation.json',
                       "JSON"),
                       ('channelnames.txt',
                        "TXTWORDLIST"),
                      ]
    
    optional_files = [('exposure_times.txt', 'CSV')]

    @classmethod
    def find_top(cls, path, target):
        """
        In some cases, all or most of the needed files are in a subdirectory.  If it
        exists, find it.
        """
        if target in os.listdir(path):
            return '.'
        else:
            candidates = [nm for nm in os.listdir(path)
                          if nm.startswith('src_')
                          and os.path.isdir(os.path.join(path, nm))]
            for cand in candidates:
                if target in os.listdir(os.path.join(path, cand)):
                    return cand
            else:
                return None

    @classmethod
    def test_match(cls, path):
        """
        Does the given path point to the top directory of a directory tree
        containing data of this collection type?
        """
        offsetdir = cls.find_top(path, cls.top_target)
        if offsetdir is None:
            return False
        for match, _ in cls.expected_files:
            print('testing %s' % match)
            if not any(glob.iglob(os.path.join(path,offsetdir, match))):
                print('not found!')
                return False
        return True
    
    def __init__(self, path):
        """
        path is the top level directory of the collection
        """
        super().__init__(path)
        self.offsetdir = self.find_top(self.topdir, self.top_target)
        assert self.offsetdir is not None, 'Wrong dataset type?'
            
    
    def collect_metadata(self):
        rslt = {}
        md_type_tbl = self.get_md_type_tbl()
        for match, md_type in self.expected_files + self.optional_files:
            print('collect match %s' % match)
            for fpath in glob.iglob(os.path.join(self.topdir, self.offsetdir, match)):
                print('collect from path %s' % fpath)
                this_md = md_type_tbl[md_type](fpath).collect_metadata()
                if this_md is not None:
                    rslt[os.path.relpath(fpath, self.topdir)] = this_md
        cl = []
        for fname in os.listdir(os.path.join(self.topdir, self.offsetdir)):
            fullname = os.path.join(self.topdir, self.offsetdir, fname)
            if os.path.isdir(fullname) and fname.startswith('cyc'):
                cl.append(fname)
        rslt['components'] = cl
        rslt['collectiontype'] = 'codex'
        return rslt
    
    def filter_metadata(self, metadata):
        """
        This extracts the metadata which is actually desired downstream from the bulk of the
        metadata which has been collected.
        
        """
        rslt = {k : metadata[k] for k in ['collectiontype', 'components']}

#         for elt in metadata:
#             # each element is the pathname of the file from which it was extracted
#             if not os.path.dirname(elt) and elt.endswith('spatial_meta.txt'):
#                 spatial_meta = metadata[elt]
#                 break
#             else:
#                 raise MetadataError('The spatial metadata is unexpectedly missing')
# 
#         rslt['ccf_spatial'] = {k : v for k, v in spatial_meta.items()}
        
        other_d = {}
        for k in metadata:
            if k not in rslt:
                other_d[k] = metadata[k]
        rslt['other_meta'] = other_d  # for debugging
        return rslt
            
        
