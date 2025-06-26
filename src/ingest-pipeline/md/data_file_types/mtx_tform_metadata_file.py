#! /usr/bin/env python

from metadata_file import MetadataFile
from type_base import MetadataError

class MtxTformMetadataFile(MetadataFile):
    """A metadata file type for files containing geometrical transforms as 4x4 matrices"""
    category_name = 'MtxTform';

    def collect_metadata(self):
        print('parsing transformation text from %s' % self.path)
        rslt = {}
        row_list = []
        with open(self.path, 'r') as f:
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
