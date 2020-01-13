#! /usr/bin/env python

import pylibczi
import xmltodict
from lxml import etree
from metadata_file import MetadataFile

class CZIMetadataFile(MetadataFile):
    """A metadata file type for CZI (Zeiss) files"""
    category_name = 'CZI';

    def collect_metadata(self):
        print('parsing czi from %s' % self.path)
        czi_file = pylibczi.CziFile(self.path, verbose=True)
        czi_file.read_meta()
        metadata = xmltodict.parse(etree.tostring(czi_file.meta_root))
        return metadata
    
