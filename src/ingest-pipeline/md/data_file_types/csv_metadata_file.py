#! /usr/bin/env python

from metadata_file import MetadataFile
import csv

class CSVMetadataFile(MetadataFile):
    """
    A metadata file type for csv files.  At the moment we are keeping it maximally dumb.
    """
    category_name = 'CSV';

    def collect_metadata(self):
        print('parsing csv from %s' % self.path)
        md = []
        with open(self.path, 'rU', newline='') as f:
            dialect = csv.Sniffer().sniff(f.read(1024))
            f.seek(0)
            reader = csv.DictReader(f, dialect=dialect)
            for row in reader:
                md.append({k : v for k, v in row.items()})
        return md
