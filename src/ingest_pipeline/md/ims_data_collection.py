#! /usr/bin/env python

import os
import json
import glob

from data_collection import DataCollection, MD_TYPE_TBL

class IMSDataCollection(DataCollection):
    category_name = 'IMS';

    # expected_file pairs are (globable name, filetype key) 
    expected_files = [('*-spatial_meta.txt',
                       "IGNORE"),
                      ('raw_microscopy/*-AF_raw.czi',
                       "IGNORE"),
                      ('raw_microscopy/*-MxIF_raw.czi',
                       "IGNORE"),
                      ('raw_microscopy/*-PAS_raw.scn',
                       "IGNORE"),
                      ('raw_microscopy/transformix_transformation_files/MxIF_transformsToIMS/*-MxIF_toIMS_tform1.txt',
                       "IGNORE"),
                      ('raw_microscopy/transformix_transformation_files/MxIF_transformsToIMS/*-MxIF_toIMS_tform2.txt',
                       "IGNORE"),
                      ('raw_microscopy/transformix_transformation_files/MxIF_transformsToIMS/*-MxIF_toIMS_tform3.txt',
                       "IGNORE"),
                      ('raw_microscopy/transformix_transformation_files/PAS_transformsToIMS/*-PAS_toIMS_tform.txt',
                       "IGNORE"),
                      ('raw_microscopy/transformix_transformation_files/preAF_transformsToIMS/*-IMS_preAF_toIMS_tform.txt',
                       "IGNORE"),
                      ('processed_microscopy/*-mxIF_toIMS.ome.tiff',
                       "IGNORE"),
                      ('processed_microscopy/*-AF_pAF_toIMS.ome.tiff',
                       "IGNORE"),
                      ('processed_microscopy/*-pas_toIMS.ome.tiff',
                       "IGNORE"),
                      ('IMS/*-instrument_metadata.yml',
                       "YAML"),
                      ('IMS/*-peak_metadata.csv',
                       "IGNORE"),
                      ('IMS/*-tform_to_microscopy_metadata.txt',
                       "IGNORE"),
                      ('IMS/columnar/*.csv',
                       "IGNORE"),
                      ('IMS/imzml/*.ibd',
                       "IGNORE"),
                      ('IMS/imzml/*.imzML',
                       "IGNORE"),
                      ('IMS/tif/*.ome.tiff',
                       "IGNORE"),
                      ('IMS/tif/individual_final/*.tiff',
                       "IGNORE"),
                      ]
    
    @classmethod
    def test_match(cls, path):
        """
        Does the given path point to the top directory of a directory tree
        containing data of this collection type?
        """
        for match, _ in cls.expected_files:
            print('testing %s' % match)
            if not any(glob.iglob(os.path.join(path,match))):
                print('not found!')
                return False
        return True
    
    def __init__(self, path):
        """
        path is the top level directory of the collection
        """
        super().__init__(path)
    
    def collect_metadata(self):
        rslt = {}
        for match, md_type in type(self).expected_files:
            print('collect match %s' % match)
            for fpath in glob.iglob(os.path.join(self.topdir, match)):
                print('collect from path %s' % fpath)
                this_md = MD_TYPE_TBL[md_type](fpath).collect_metadata()
                if this_md is not None:
                    rslt[fpath] = this_md
        return rslt
