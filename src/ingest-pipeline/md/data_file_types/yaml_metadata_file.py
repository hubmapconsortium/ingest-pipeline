#! /usr/bin/env python

from metadata_file import MetadataFile
import yaml

class YamlMetadataFile(MetadataFile):
    """A metadata file type for yaml files"""
    category_name = 'Yaml'

    def collect_metadata(self):
        print('parsing yaml from %s' % self.path)
        with open(self.path, 'r') as f:
            md = yaml.safe_load(f)
        return md
