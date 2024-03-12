#! /usr/bin/env python

import os
import re
from datetime import datetime

from .akoya_codex_data_collection import AkoyaCODEXDataCollection


class StanfordCODEXDataCollection(AkoyaCODEXDataCollection):
    category_name = "STANFORD_CODEX"
    match_priority = 1.0  # >= 0.0; higher is better
    top_target = "processingOptions.json"
    dir_regex = re.compile(".*")

    # expected_file pairs are (globable name, filetype key)
    expected_files = [
        ("*-metadata.tsv", "METADATATSV"),
        ("{offsetdir}/processingOptions.json", "JSON"),
        ("{offsetdir}/Experiment.json", "JSON"),
        ("{offsetdir}/channelNames.txt", "TXTWORDLIST"),
    ]

    optional_files = []

    def collect_metadata(self, component=None, component_process=None):
        rslt = super(StanfordCODEXDataCollection, self).collect_metadata()
        hande_cl = []
        for fname in os.listdir(os.path.join(self.topdir, self.offsetdir)):
            fullname = os.path.join(self.topdir, self.offsetdir, fname)
            if os.path.isdir(fullname) and fname.startswith("HandE_"):
                hande_cl.append(fname)
        rslt["hande_components"] = hande_cl
        rslt["collectiontype"] = "codex"

        return rslt

    def filter_metadata(self, raw_metadata):
        """
        This extracts the metadata which is actually desired downstream from the bulk of the
        metadata which has been collected.

        """
        rslt = self.basic_filter_metadata(raw_metadata)
        expt_json_path = "{}/Experiment.json".format(self.offsetdir)
        assert expt_json_path in raw_metadata, "Experiment.json not where it should be?"
        channel_names_pth = "{}/channelNames.txt".format(self.offsetdir)
        assert channel_names_pth in raw_metadata, "channelNames.txt not where it should be?"
        tests = [
            ("resolution_x_value", "per_pixel_XY_resolution", float),
            ("resolution_y_value", "per_pixel_XY_resolution", float),
            ("resolution_z_value", "z_pitch", float),
            ("number_of_cycles", "cycle_upper_limit", int),
            ("execution_datetime", "date", datetime),
            # ("acquisition_instrument_model", "codex_instrument", str),
        ]
        # Disable internal consistency checks because we trust metadata.tsv
        # self.internal_consistency_checks(rslt, raw_metadata[expt_json_path], tests,
        #                                 None)  #raw_metadata[channel_names_pth])
        # if 'other_meta' in rslt and 'hande_components' in rslt['other_meta']:
        #     del rslt['other_meta']['hande_components']

        return rslt
