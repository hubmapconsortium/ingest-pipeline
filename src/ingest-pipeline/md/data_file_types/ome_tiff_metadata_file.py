#! /usr/bin/env python

import xmltodict
from metadata_file import MetadataFile
import tifffile

class OMETiffMetadataFile(MetadataFile):
    """A metadata file type for OME-Tiff files"""
    category_name = 'OME_TIFF';

    def collect_metadata(self):
        print('parsing OME_TIFF from %s' % self.path)
        with tifffile.TiffFile(self.path) as tf:
            metadata = xmltodict.parse(tf.ome_metadata)
        return metadata
    
