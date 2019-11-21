#! /usr/bin/env python

import numpy as np
from pyimzml.ImzMLParser import ImzMLParser
from metadata_file import MetadataFile

class ImzMLMetadataFile(MetadataFile):
    """A metadata file type for imzML files"""
    category_name = 'imzML';

    def collect_metadata(self):
        print('parsing imzML from %s' % self.path)
        with ImzMLParser(self.path) as parser:
            md = parser.imzmldict
        md = {k : (int(v) if type(v) == np.int64 else v) for k, v in md.items()}
#         for k, v in md.items():
#             print(k, v, type(v))
        return md
    
