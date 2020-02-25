#! /usr/bin/env python

import os
import json
import glob
import pandas as pd
import numpy as np
import types

from data_collection import DataCollection

class RNASEQ10XDataCollection(DataCollection):
    category_name = 'RNASEQ10X';

    # expected_file pairs are (globable name, filetype key)
    expected_files = [('*_I1_*.fastq.gz', "FASTQ"),
                      ('*_R1_*.fastq.gz', "FASTQ"),
                      ('*_R1_*.fastq.gz', "FASTQ")]
    
    optional_files = []
    
    @classmethod
    def test_match(cls, path):
        """
        Does the given path point to the top directory of a directory tree
        containing data of this collection type?
        """
        top_elts = os.listdir(path)
        if len(top_elts) == 1 and os.path.isdir(os.path.join(path,top_elts[0])):
            top_dir = top_elts[0]
            inner_path = os.path.join(path, top_dir)
            grp_elts = os.listdir(inner_path)
            if 'README.csv' in grp_elts:
                with open(os.path.join(inner_path, 'README.csv'), 'r') as f:
                    line = f.readline()
                    if '10x_Genomics_Index_well_ID' not in line:
                        print('README.csv does not match expected format')
                        return False
                    grp_elts.remove('README.csv')
                    for inner_dir in grp_elts:
                        ii_path = os.path.join(inner_path, inner_dir)
                        valid_ii_paths = 0
                        if os.path.isdir(ii_path):
                            for match, _ in cls.expected_files:
                                print('testing %s' % match)
                                if not any(glob.iglob(os.path.join(ii_path,match))):
                                    print('not found!')
                                    return False
                            valid_ii_paths += 1
                        if valid_ii_paths:
                            return True
                        else:
                            print('No valid inner directories found')
                            return(False)
            else:
                print('no README.csv found')
                return False
        else:
            print('top entry is not a single directory')
            return False
    
    def __init__(self, path):
        """
        path is the top level directory of the collection
        """
        super().__init__(path)
    
    def collect_metadata(self):
        md_type_tbl = self.get_md_type_tbl()
        top_dir = os.listdir(self.topdir)[0]
        rslt = {'collectiontype' : 'rnaseq_10x',
                'tmc_uuid' : top_dir,
                'components' : {}}
        inner_path = os.path.join(self.topdir, top_dir)
        grp_elts = os.listdir(inner_path)
        grp_elts.remove('README.csv')
        readme_df = pd.read_csv(os.path.join(inner_path, 'README.csv'))
        assert len(readme_df) == 1, 'Unexpectedly found too many lines in README.csv'
        rslt['components']['README.csv'] = {k : readme_df.loc[0][k] for k in readme_df.columns}
        for inner_dir in grp_elts:
            ii_path = os.path.join(inner_path, inner_dir)
            if os.path.isdir(ii_path):
                ii_rslt = {}
                for match, md_type in self.expected_files + self.optional_files:
                    for fpath in glob.iglob(os.path.join(self.topdir, match)):
                        this_md = md_type_tbl[md_type](fpath).collect_metadata()
                        if this_md is not None:
                            ii_rslt[os.path.relpath(fpath, inner_path)] = this_md
                rslt['components'][inner_dir] = ii_rslt
        return rslt
    
    def filter_metadata(self, metadata):
        """
        This extracts the metadata which is actually desired downstream from the bulk of the
        metadata which has been collected.
        
        """
        rslt = {}
        for k, v in metadata.items():
            if k != 'components':
                rslt[k] = v
        component_l = []
        for component, sub_meta in metadata['components'].items():
            if component == 'README.csv':
                sub_rslt = {}
                for k, v in sub_meta.items():
                    if isinstance(v, (np.int8, np.int32, np.int64)):
                        sub_rslt[k] = int(v)
                    elif isinstance(v, np.float):
                        if not np.isnan(v):
                            sub_rslt[k] = float(v)
                    elif isinstance(v, str):
                        while v.startswith("'") and v.endswith("'"):
                            v = v[1:-1]
                        sub_rslt[k] = v
                    else:
                        sub_rslt[k] = repr(v)
                rslt['dataset'] = sub_rslt
            else:
                assert sub_meta == {}, 'Expected only empty metadata below this level'
                component_l.append(component)
        component_l.sort()
        rslt['components'] = component_l
            
        return rslt
            
        
