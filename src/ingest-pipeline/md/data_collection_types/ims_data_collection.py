#! /usr/bin/env python

import os
import json
import glob

from data_collection import DataCollection

class IMSDataCollection(DataCollection):
    category_name = 'IMS';

    # expected_file pairs are (globable name, filetype key)
    expected_files = [('*-spatial_meta.txt',
                       "JSON"),
                      ('raw_microscopy/*-AF_raw.czi',
                       "CZI"),
                      ('raw_microscopy/*-MxIF_raw.czi',
                       "CZI"),
                      ('raw_microscopy/*-PAS_raw.scn',
                       "SCN_TIFF"),
                      ('raw_microscopy/transformix_transformation_files/MxIF_transformsToIMS/*-MxIF_toIMS_tform1.txt',
                       "TXTTFORM"),
                      ('raw_microscopy/transformix_transformation_files/MxIF_transformsToIMS/*-MxIF_toIMS_tform2.txt',
                       "TXTTFORM"),
                      ('raw_microscopy/transformix_transformation_files/MxIF_transformsToIMS/*-MxIF_toIMS_tform3.txt',
                       "TXTTFORM"),
                      ('raw_microscopy/transformix_transformation_files/PAS_transformsToIMS/*-PAS_toIMS_tform.txt',
                       "TXTTFORM"),
                      ('raw_microscopy/transformix_transformation_files/preAF_transformsToIMS/*-IMS_preAF_toIMS_tform.txt',
                       "TXTTFORM"),
                      ('processed_microscopy/*-mxIF_toIMS.ome.tiff',
                       "OME_TIFF"),
                      ('processed_microscopy/*-AF_pAF_toIMS.ome.tiff',
                       "OME_TIFF"),
                      ('processed_microscopy/*-pas_toIMS.ome.tiff',
                       "OME_TIFF"),
                      ('IMS/*-instrument_metadata.yml',
                       "YAML"),
                      ('IMS/*-peak_metadata.csv',
                       "IGNORE"),
                      ('IMS/*-tform_to_microscopy_metadata.txt',
                       "MTXTFORM"),
                      ('IMS/columnar/*.csv',
                       "IGNORE"),
                      ('IMS/imzml/*.ibd',
                       "IGNORE"),
                      ('IMS/imzml/*.imzML',
                       "IMZML"),
                      ('IMS/tif/*.ome.tiff',
                       "OME_TIFF"),
                      ('IMS/tif/individual_final/*.tiff',
                       "OME_TIFF"),
                      ]
    
    optional_files = []
    
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
        md_type_tbl = self.get_md_type_tbl()
        for match, md_type in self.expected_files + self.optional_files:
            #print('collect match %s' % match)
            for fpath in glob.iglob(os.path.join(self.topdir, match)):
                #print('collect from path %s' % fpath)
                this_md = md_type_tbl[md_type](fpath).collect_metadata()
                if this_md is not None:
                    rslt[os.path.relpath(fpath, self.topdir)] = this_md
        return rslt
    
    def filter_metadata(self, metadata):
        """
        This extracts the metadata which is actually desired downstream from the bulk of the
        metadata which has been collected.
        
        """
        rslt = {}
        for elt in metadata:
            # each element is the pathname of the file from which it was extracted
            if not os.path.dirname(elt) and elt.endswith('spatial_meta.txt'):
                spatial_meta = metadata[elt]
                break
            else:
                raise MetadataError('The spatial metadata is unexpectedly missing')

        rslt['ccf_spatial'] = {k : v for k, v in spatial_meta.items()}
        
        #rslt['other_meta'] = metadata.copy()  # for debugging
        return rslt
            
        
