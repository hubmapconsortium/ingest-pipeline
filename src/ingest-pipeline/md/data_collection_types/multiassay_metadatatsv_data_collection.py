#! /usr/bin/env python

"""
This data collection type expects a single metadata.tsv file at top level.
It is intended as a convenience for developers.
"""

import os
import glob
import json

import requests
from requests import codes
from requests.exceptions import HTTPError
from type_base import MetadataError
from data_collection import DataCollection


class MultiassayMetadataTSVDataCollection(DataCollection):
    category_name = 'MULTIASSAYMETADATATSV';
    match_priority = 2.0  # >= 0.0; higher is better
    top_target = None
    dir_regex = None

    # expected_file pairs are (globable name, filetype key)
    expected_files = [('*-metadata.tsv', 'METADATATSV')]

    optional_files = []

    @classmethod
    def find_top(cls, path, target, dir_regex=None):
        """
        For this data collection, there is expected to be only a single directory
        containing the metadata.tsv file.
        """
        return '.'

    @classmethod
    def test_match(cls, path):
        """
        Does the given path point to the top directory of a directory tree
        containing data of this collection type?
        """
        offsetdir = cls.find_top(path, cls.top_target, cls.dir_regex)
        print('Checking for multiple metadata.tsv at top level')
        if offsetdir is None:
            return False
        candidates = os.listdir(os.path.join(path, offsetdir))
        return (len(candidates) > 1 and candidates[0].endswith('-metadata.tsv'))

    def __init__(self, path):
        """
        path is the top level directory of the collection
        """
        super().__init__(path)
        self.offsetdir = self.find_top(self.topdir, self.top_target, self.dir_regex)
        assert self.offsetdir is not None, 'Wrong dataset type?'

    def collect_metadata(self):
        auth_tok = os.getenv('AUTH_TOK')
        ingest_api_url = os.getenv('INGEST_API_URL')
        md_type_tbl = self.get_md_type_tbl()
        rslt = {}
        cl = []
        for match, md_type in self.expected_files + self.optional_files:
            print('collect match %s' % match.format(offsetdir=self.offsetdir))
            for fpath in glob.iglob(os.path.join(self.topdir,
                                                 match.format(offsetdir=self.offsetdir))):
                print('collect from path %s' % fpath)
                this_md = md_type_tbl[md_type](fpath).collect_metadata()
                fname = os.path.basename(fpath)

                if this_md is not None:
                    # Send the metadata file through to the assay classifier to see whether it's the multi-assay metadata or not.
                    # If it is a multi-assay, cl.extend
                    rslt[os.path.relpath(fpath, self.topdir)] = this_md
                    headers = {
                        "authorization": f"Bearer {auth_tok}",
                        'content-type': 'application/json',
                        'X-Hubmap-Application': 'ingest-pipeline',
                    }

                    try:
                        response = requests.post(f'{ingest_api_url}assaytype', headers=headers,
                                                 data=json.dumps(this_md[0]))
                        response = response.json()
                    except HTTPError as e:
                        if e.response.status_code == codes.unauthorized:
                            raise RuntimeError('ingest_api_connection authorization was rejected?')
                        else:
                            print('benign error')
                            return None

                    if 'metadata' in fname and fname.endswith('.tsv'):
                        assert isinstance(this_md, list), 'metadata.tsv did not produce a list'
                        if 'must-contain' in response:
                            print('MULTI ASSAY FOUND')
                            cl.extend(this_md)
                        else:
                            print('NON MULTI ASSAY FOUND')
                            print(this_md)


        rslt['components'] = cl
        rslt['collectiontype'] = 'multiassay_metadatatsv'
        return rslt

    def basic_filter_metadata(self, raw_metadata):
        """
        Make sure basic components of metadata are present, and promote them
        """
        # We need to grab the right metadata file...
        rslt = {k: raw_metadata[k] for k in ['collectiontype']}
        if len(raw_metadata['components']) != 1:
            raise MetadataError("Only one line of metadata.tsv info is currently supported")
        rslt.update(raw_metadata['components'][0])

        return rslt

    def filter_metadata(self, raw_metadata):
        """
        This extracts the metadata which is actually desired downstream from the bulk of the
        metadata which has been collected.

        """
        rslt = self.basic_filter_metadata(raw_metadata)

        return rslt
