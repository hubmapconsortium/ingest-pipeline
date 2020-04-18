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
    expected_files = [('*-metadata.tsv', 'METADATATSV'),
                      ('{offsetdir}/experiment.json', "JSON"),
                      ('{offsetdir}/segmentation.json', "JSON")
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
            print('testing %s' % match.format(offsetdir=offsetdir))
            if not any(glob.iglob(os.path.join(path,match.format(offsetdir=offsetdir)))):
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
        cl = []
        for match, md_type in self.expected_files + self.optional_files:
            print('collect match %s' % match.format(offsetdir=self.offsetdir))
            for fpath in glob.iglob(os.path.join(self.topdir,
                                                 match.format(offsetdir=self.offsetdir))):
                print('collect from path %s' % fpath)
                this_md = md_type_tbl[md_type](fpath).collect_metadata()
                if this_md is not None:
                    rslt[os.path.relpath(fpath, self.topdir)] = this_md
                    fname = os.path.basename(fpath)
                    if 'metadata' in fname and fname.endswith('.tsv'):
                        assert isinstance(this_md, list), 'metadata...tsv did not produce a list'
                        cl.extend(this_md)
        rslt['components'] = cl
        rslt['collectiontype'] = 'codex'
        return rslt
    
    def filter_metadata(self, metadata):
        """
        This extracts the metadata which is actually desired downstream from the bulk of the
        metadata which has been collected.
        
        """
        rslt = {k : metadata[k] for k in ['collectiontype', 'components']}

        other_d = {}
        for k in metadata:
            if k not in rslt:
                other_d[k] = metadata[k]
        rslt['other_meta'] = other_d  # for debugging
        return rslt
            
        
