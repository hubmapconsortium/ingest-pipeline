#! /usr/bin/env python

from metadata_file import MetadataFile

class TxtWordListMetadataFile(MetadataFile):
    """A metadata file type containing text to be returned as a list of words"""
    category_name = 'TxtWordList'

    def collect_metadata(self):
        print('collecting words from %s' % self.path)
        rslt_l = []
        with open(self.path, 'r') as f:
            for line in f:
                line = line.strip()
                words = line.split()
                rslt_l += words
        return rslt_l
