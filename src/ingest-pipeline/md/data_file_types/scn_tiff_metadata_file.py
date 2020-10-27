#! /usr/bin/env python

import xmltodict
from metadata_file import MetadataFile
import tifffile

class ScnTiffMetadataFile(MetadataFile):
    """A metadata file type for Scn-Tiff files"""
    category_name = 'Scn_TIFF';

    def collect_metadata(self):
        print('parsing Scn_TIFF from %s' % self.path)
        with tifffile.TiffFile(self.path) as tf:
            # tifffile.TiffFile.scn_metadata seems to be missing
            metadata = xmltodict.parse(tf.pages[0].description) if tf.is_scn else None
        return metadata
    
