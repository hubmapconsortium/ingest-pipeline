#! /usr/bin/env python

from metadata_file import MetadataFile
from type_base import MetadataError
import csv
from pprint import pprint

class TSVMetadataFile(MetadataFile):
    """
    A metadata file type specialized for tsv files, since the csv sniffer often fails
    """
    category_name = 'TSV';

    def collect_metadata(self):
        print('parsing csv from %s' % self.path)
        md = []
        try:
            with open(self.path, 'rU', newline='', encoding='ascii') as f:
                reader = csv.DictReader(f, delimiter='\t')
                for row in reader:
                    md.append({k : v for k, v in row.items()})
        except UnicodeDecodeError as e:
            raise MetadataError(str(e) + f'in {self.path}')
        return md
