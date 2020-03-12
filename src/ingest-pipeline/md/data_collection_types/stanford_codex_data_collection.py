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
        rslt = super(StanfordCODEXDataCollection, self).filter_metadata(metadata)
        rslt['hande_components'] = metadata['hande_components']
        if 'other_meta' in rslt and 'hande_components' in rslt['other_meta']:
            del rslt['other_meta']['hande_components']

        return rslt
            
        
