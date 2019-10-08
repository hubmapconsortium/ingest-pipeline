#! /usr/bin/env python

from metadata_file import MetadataFile
from type_base import MetadataError

class MtxTformMetadataFile(MetadataFile):
    """A metadata file type for files containing geometrical transforms as 4x4 matrices"""
    category_name = 'MtxTform';

    def __init__(self, path):
        """
        path is the top level directory of the collection
        """
        self.path = path
    
    def __str__(self):
        return '<%s MetadataFile>' % self.category_name

    def __repr__(self):
        return '<%s(%s)>' % (type(self).__name__, self.path)

    def collect_metadata(self):
        print('parsing transformation text from %s' % self.path)
        rslt = {}
        row_list = []
        with open(self.path, 'rU') as f:
            for line in f:
                line = line.strip()
                words = line.split()
                try:
                    words = [float(word) for word in words]
                except ValueError:
                    raise MetadataError("Unexpected format line <{}>".format(line))
                row_list.append(words[:])
        assert len(row_list) == 4, "Unexpected format line <{}>".format(line)
        rslt['Transform'] = row_list
        return rslt
