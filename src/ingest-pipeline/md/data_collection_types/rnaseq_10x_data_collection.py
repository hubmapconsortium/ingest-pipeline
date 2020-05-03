#! /usr/bin/env python

import os
import json
import glob
#import pandas as pd
#import numpy as np
import types
import re
from pprint import pprint

from type_base import MetadataError
from data_collection import DataCollection

class RNASEQ10XDataCollection(DataCollection):
    category_name = 'RNASEQ10X';
    top_target = 'README.csv'
    dir_regex = re.compile('raw_.*')

    # expected_file pairs are (globable name, filetype key)
    expected_files = [('*-metadata.tsv', 'METADATATSV'),
                      ('{offsetdir}/README.csv', 'CSV'),
                      ('{offsetdir}/*/*_I1_*.fastq.gz', "FASTQ"),
                      ('{offsetdir}/*/*_R1_*.fastq.gz', "FASTQ"),
                      ('{offsetdir}/*/*_R1_*.fastq.gz', "FASTQ")]
    
    optional_files = []

    @classmethod
    def find_top(cls, path, target, dir_regex=None):
        """
        In some cases, all or most of the needed files are in a subdirectory.  If it
        exists, find it.
        """
        if target in os.listdir(path):
            return '.'
        else:
            if dir_regex is None:
                candidates = [nm for nm in os.listdir(path)
                              if os.path.isdir(os.path.join(path, nm))]
            else:
                candidates = [nm for nm in os.listdir(path)
                              if os.path.isdir(os.path.join(path, nm)) and dir_regex.match(nm)]
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
        offsetdir = cls.find_top(path, cls.top_target, cls.dir_regex)
        if offsetdir is None:
            return False
        with open(os.path.join(path, offsetdir, cls.top_target)) as f:
            line = f.readline()
            if '10x_Genomics_Index_well_ID' not in line:
                print('README.csv does not match expected format')
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
        self.offsetdir = self.find_top(self.topdir, self.top_target, self.dir_regex)
        assert self.offsetdir is not None, 'Wrong dataset type?'
    
    def collect_metadata(self):
        md_type_tbl = self.get_md_type_tbl()
        rslt = {'collectiontype' : 'rnaseq_10x',
                'components' : []}
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
        rslt['collectiontype'] = 'rnaseq_10x'
        return rslt
    
    def basic_filter_metadata(self, raw_metadata):
        """
        Make sure basic components of metadata are present, and promote them
        """
        rslt = {k : raw_metadata[k] for k in ['collectiontype']}
        if len(raw_metadata['components']) != 1:
            raise MetadataError("Only one line of metadata.tsv info is currently supported")
        rslt.update(raw_metadata['components'][0])
        
        # Hard-coded reality checks
        if 'assay_type' not in rslt or rslt['assay_type'] != 'scRNA-Seq-10xGenomics':
            raise MetadataError('assay_type is not ' 'scRNA-Seq-10xGenomics')

        return rslt

    def internal_consistency_checks(self, rslt, readme_md):
        """
        Check a variety of relationships that are believed to hold between [Ee]xperiment.json and
        metadata found in the metadata.tsv file.
        """
        if 'tissue_id' not in rslt:
            raise MetadataError('metadata is missing tissue_id')
        if 'UUID Identifier' not in readme_md:
            raise MetadataError('README metadata is missing UUID Identifier')
        if rslt['tissue_id'] != readme_md['UUID Identifier']:
            raise MetadataError('tissue_id does not match UUID Identifier')

    def filter_metadata(self, raw_metadata):
        """
        This extracts the metadata which is actually desired downstream from the bulk of the
        metadata which has been collected.
        
        """
        rslt = self.basic_filter_metadata(raw_metadata)

        candidate_l = [raw_metadata[k][0] for k in raw_metadata if 'README.csv' in k] 
        if not candidate_l:
            raise MetadataError('Cannot find README.csv')
        
        readme_md = candidate_l[0]
        for elt in candidate_l[1:]:
            if elt != experiment_md:
                raise MetadataError("Multiple README.csv files that do not match")

        self.internal_consistency_checks(rslt, readme_md)

        rslt['other_metadata'] = readme_md
            
        return rslt
            
        
