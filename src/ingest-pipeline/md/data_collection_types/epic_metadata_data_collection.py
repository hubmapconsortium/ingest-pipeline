#! /usr/bin/env python

"""
This data collection type expects a single metadata.tsv file at top level.
It is intended as a convenience for developers.
"""

import os
import glob

from type_base import MetadataError
from data_collection import DataCollection


class EpicMetadataTSVDataCollection(DataCollection):
    category_name = "EPICMETADATATSV"
    match_priority = 2.1  # >= 0.0; higher is better
    top_target = None
    dir_regex = None

    # expected_file pairs are (globable name, filetype key)
    expected_files = [("*metadata.tsv", "METADATATSV"),
                      ("derived/*/*.ome.tiff", "OME_TIFF")]

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
        print("Checking for metadata.tsv and derived directory for ome-tiff files at top level")
        if offsetdir is None:
            return False
        for match, _ in cls.expected_files:
            print("testing %s" % match.format(offsetdir=offsetdir))
            if not any(glob.iglob(os.path.join(path, match.format(offsetdir=offsetdir)))):
                print("not found!")
                return False
        return True

    def __init__(self, path):
        """
        path is the top level directory of the collection
        """
        super().__init__(path)
        self.offsetdir = self.find_top(self.topdir, self.top_target, self.dir_regex)
        assert self.offsetdir is not None, "Wrong dataset type?"

    def collect_metadata(self, component=None, component_process=None):
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
                if this_md is not None:
                    rslt[os.path.relpath(fpath, self.topdir)] = this_md
                    fname = os.path.basename(fpath)
                    if "metadata" in fname and fname.endswith(".tsv"):
                        assert isinstance(this_md, list), "metadata.tsv did not produce a list"
                        rec_list = this_md
                        for rec in rec_list:
                            assert "derived_dataset_type" in rec, ("No derived_dataset_type found "
                                                                   "in metadata.tsv")
                            for key in ["data_path", "contributors_path"]:
                                assert (
                                    key in rec
                                ), 'metadata.tsv does not have a "{}" column'.format(key)
                            this_dict = {"metadata": rec}
                            for sub_key, dict_key in [
                                ("contributors_path", "contributors"),
                                ("antibodies_path", "antibodies"),
                            ]:
                                if sub_key in rec:
                                    assert rec[sub_key].endswith(
                                        ".tsv"
                                    ), 'TSV file expected, received "{}"'.format(rec[sub_key])
                                    sub_path = os.path.join(os.path.dirname(fpath), rec[sub_key])
                                    sub_parser = md_type_tbl["TSV"](sub_path)
                                    sub_md = sub_parser.collect_metadata()
                                    this_dict[dict_key] = sub_md
                            cl.append(this_dict)

        rslt["components"] = cl
        rslt["collectiontype"] = "epic_metadatatsv"
        return rslt

    def basic_filter_metadata(self, raw_metadata):
        """
        Make sure basic components of metadata are present, and promote them
        """
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
