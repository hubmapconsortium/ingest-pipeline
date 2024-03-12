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
import urllib.parse as urlparser


class MultiassayMetadataTSVDataCollection(DataCollection):
    category_name = "MULTIASSAYMETADATATSV"
    match_priority = 2.1  # >= 0.0; higher is better
    top_target = None
    dir_regex = None

    # expected_file pairs are (globable name, filetype key)
    expected_files = [("*-metadata.tsv", "METADATATSV")]

    optional_files = []

    @classmethod
    def find_top(cls, path, target, dir_regex=None):
        """
        For this data collection, there is expected to be only a single directory
        containing the metadata.tsv file.
        """
        return "."

    @classmethod
    def test_match(cls, path):
        """
        Does the given path point to the top directory of a directory tree
        containing data of this collection type?
        """
        offsetdir = cls.find_top(path, cls.top_target, cls.dir_regex)
        print("Checking for multiple metadata.tsv at top level")
        if offsetdir is None:
            return False
        files = os.listdir(os.path.join(path, offsetdir))
        candidates = []
        for c in files:
            if c.endswith("-metadata.tsv"):
                candidates.append(c)
        return len(candidates) > 1

    def __init__(self, path):
        """
        path is the top level directory of the collection
        """
        super().__init__(path)
        self.offsetdir = self.find_top(self.topdir, self.top_target, self.dir_regex)
        assert self.offsetdir is not None, "Wrong dataset type?"

    def collect_metadata(self, component=None, component_process=None):
        ingest_api_url = urlparser.unquote(os.getenv("INGEST_API_URL")).split("http://")[1]
        md_type_tbl = self.get_md_type_tbl()
        rslt = {}
        cl = []
        for match, md_type in self.expected_files + self.optional_files:
            print("collect match %s" % match.format(offsetdir=self.offsetdir))
            for fpath in glob.iglob(
                os.path.join(self.topdir, match.format(offsetdir=self.offsetdir))
            ):
                print("collect from path %s" % fpath)
                this_md = md_type_tbl[md_type](fpath).collect_metadata()
                fname = os.path.basename(fpath)

                if this_md is not None:
                    # Send the metadata file through to the assay classifier to see whether it's the multi-assay
                    # metadata or not.
                    # If it is a multi-assay, cl.extend
                    rslt[os.path.relpath(fpath, self.topdir)] = this_md
                    headers = {
                        "content-type": "application/json",
                        "X-SenNet-Application": "ingest-pipeline",
                    }

                    try:
                        response = requests.post(
                            f"{ingest_api_url}assaytype",
                            headers=headers,
                            data=json.dumps(this_md[0]),
                        )
                        response = response.json()
                    except HTTPError as e:
                        if e.response.status_code == codes.unauthorized:
                            raise RuntimeError("ingest_api_connection authorization was rejected?")
                        else:
                            print("benign error")
                            return None

                    if "metadata" in fname and fname.endswith(".tsv"):
                        assert isinstance(this_md, list), "metadata.tsv did not produce a list"
                        if "must-contain" in response and component_process is None:
                            print("MULTI ASSAY FOUND")
                            for rec in this_md:
                                this_dict = {"metadata": rec}
                                for sub_key, dict_key in [
                                    ("contributors_path", "contributors"),
                                    ("antibodies_path", "antibodies"),
                                ]:
                                    if sub_key in rec:
                                        assert rec[sub_key].endswith(
                                            ".tsv"
                                        ), 'TSV file expected, received "{}"'.format(rec[sub_key])
                                        sub_path = os.path.join(
                                            os.path.dirname(fpath), rec[sub_key]
                                        )
                                        sub_parser = md_type_tbl["TSV"](sub_path)
                                        sub_md = sub_parser.collect_metadata()
                                        this_dict[dict_key] = sub_md
                                cl.append(this_dict)
                                print(this_dict)
                        elif component_process is not None and component == response.get("dataset-type"):
                            for rec in this_md:
                                this_dict = {"metadata": rec}
                                for sub_key, dict_key in [
                                    ("contributors_path", "contributors"),
                                    ("antibodies_path", "antibodies"),
                                ]:
                                    if sub_key in rec:
                                        assert rec[sub_key].endswith(
                                            ".tsv"
                                        ), 'TSV file expected, received "{}"'.format(rec[sub_key])
                                        sub_path = os.path.join(
                                            os.path.dirname(fpath), rec[sub_key]
                                        )
                                        sub_parser = md_type_tbl["TSV"](sub_path)
                                        sub_md = sub_parser.collect_metadata()
                                        this_dict[dict_key] = sub_md
                                cl.append(this_dict)
                                print(this_dict)
                        else:
                            print("NON MULTI ASSAY FOUND")
                        print(this_md)

        rslt["components"] = cl
        rslt["collectiontype"] = "multiassay_metadatatsv"
        return rslt

    def basic_filter_metadata(self, raw_metadata):
        """
        Make sure basic components of metadata are present, and promote them
        """
        # We need to grab the right metadata file...
        rslt = {k: raw_metadata[k] for k in ["collectiontype"]}
        if len(raw_metadata["components"]) != 1:
            raise MetadataError("Only one line of metadata.tsv info is currently supported")
        rslt.update(raw_metadata["components"][0])

        return rslt

    def filter_metadata(self, raw_metadata):
        """
        This extracts the metadata which is actually desired downstream from the bulk of the
        metadata which has been collected.

        """
        rslt = self.basic_filter_metadata(raw_metadata)

        return rslt
