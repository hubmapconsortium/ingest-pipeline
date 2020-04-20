#! /usr/bin/env python

import os
import json
import glob
from datetime import datetime, timedelta
import pytz
from pprint import pprint

from type_base import MetadataError
from data_collection import DataCollection


def inner_close_enough_match(v1, v2, tp):
    """
    Converts both values to the given type, and returns True if they match closely enough to
    satisfy consistency rules for metadata.tsv, false otherwise.
    """
    if tp == str:
        return str(v1) == str(v2)
    elif tp == float:
        f1 = float(v1)
        f2 = float(v2)
        return abs(f1 - f2)/(abs(f1) + abs(f2)) < 0.00001
    elif tp == int:
        return int(v1) == int(v2)
    elif tp == datetime:
        if '[' in v1:
            v1 = v1[:v1.find('[')]
        if '[' in v2:
            timezone = pytz.timezone(v2[v2.find('[')+1 : v2.find(']')])
            v2 = v2[:v2.find('[')]
        else:
            timezone = pytz.utc
        print('timezone: ', timezone)
        d2 = datetime.strptime(v2, '%Y-%m-%dT%H:%M:%S.%f%z')
        d1 = datetime.strptime(v1, '%Y-%m-%d %H:%M')
        d1 = timezone.localize(d1)
        dlt = d1 - d2
        print('timedelta: ', dlt)
        return abs(dlt).total_seconds() <= 3600.0  # We will allow up to an hour delta
    else:
        raise MetadataError('close_enough_match does not know how to compare values of type {}'
                            .format(tp.__name__))


def close_enough_match(v1, v2, tp):
    rslt = inner_close_enough_match(v1, v2, tp)
    print('{} {} {} -> {}'.format(v1, v2, tp.__name__, rslt))
    return True

class AkoyaCODEXDataCollection(DataCollection):
    category_name = 'AKOYA_CODEX';
    top_target = 'experiment.json'

    # expected_file pairs are (globable name, filetype key)
    expected_files = [('*-metadata.tsv', 'METADATATSV'),
                      ('{offsetdir}/experiment.json', "JSON"),
                      ('{offsetdir}/segmentation.json', "JSON")
                      ]
    
    optional_files = [('exposure_times.txt', 'CSV')]

    @classmethod
    def find_top(cls, path, target):
        """
        In some cases, all or most of the needed files are in a subdirectory.  If it
        exists, find it.
        """
        if target in os.listdir(path):
            return '.'
        else:
            candidates = [nm for nm in os.listdir(path)
                          if nm.startswith('src_')
                          and os.path.isdir(os.path.join(path, nm))]
            for cand in candidates:
                if target in os.listdir(os.path.join(path, cand)):
                    return cand
            else:
                return None

    @classmethod
    def test_match(cls, path):
        """
        Does the given path point to the top directory of a directory tree
        containing data of this collection type?
        """
        offsetdir = cls.find_top(path, cls.top_target)
        if offsetdir is None:
            return False
        for match, _ in cls.expected_files:
            print('testing %s' % match.format(offsetdir=offsetdir))
            if not any(glob.iglob(os.path.join(path,match.format(offsetdir=offsetdir)))):
                print('not found!')
                return False
        return True
    
    def __init__(self, path):
        """
        path is the top level directory of the collection
        """
        super().__init__(path)
        self.offsetdir = self.find_top(self.topdir, self.top_target)
        assert self.offsetdir is not None, 'Wrong dataset type?'
            
    
    def collect_metadata(self):
        rslt = {}
        md_type_tbl = self.get_md_type_tbl()
        cl = []
        for match, md_type in self.expected_files + self.optional_files:
            print('collect match %s' % match.format(offsetdir=self.offsetdir))
            for fpath in glob.iglob(os.path.join(self.topdir,
                                                 match.format(offsetdir=self.offsetdir))):
                print('collect from path %s' % fpath)
                this_md = md_type_tbl[md_type](fpath).collect_metadata()
                if this_md is not None:
                    rslt[os.path.relpath(fpath, self.topdir)] = this_md
                    fname = os.path.basename(fpath)
                    if 'metadata' in fname and fname.endswith('.tsv'):
                        assert isinstance(this_md, list), 'metadata...tsv did not produce a list'
                        cl.extend(this_md)
        rslt['components'] = cl
        rslt['collectiontype'] = 'codex'
        return rslt

    def filter_metadata(self, raw_metadata):
        """
        This extracts the metadata which is actually desired downstream from the bulk of the
        metadata which has been collected.
        
        """
        rslt = {k : raw_metadata[k] for k in ['collectiontype']}

        if len(raw_metadata['components']) != 1:
            raise MetadataError("Only one line of metadata.tsv info is currently supported")
        rslt.update(raw_metadata['components'][0])
        
        # Hard-coded reality checks
        if 'assay_type' not in rslt or rslt['assay_type'] != 'CODEX':
            raise MetadataError('assay_type is not CODEX')

        candidate_l = [raw_metadata[k] for k in raw_metadata if 'experiment.json' in k]
        if not candidate_l:
            raise MetadataError('Cannot find experiment.json')
        
        experiment_md = candidate_l[0]
        for elt in candidate_l[1:]:
            if elt != experiment_md:
                raise MetadataError("Multiple experiment.json files that do not match")
                
        for rslt_nm, expt_nm in [("acquisition_instrument_model", "version", ),
                                 ("resolution_x_value", "xyResolution"),
                                 ("resolution_y_value", "xyResolution"),
                                 ("resolution_z_value", "zPitch"),
                                 ("number_of_cycles", "cycle_upper_limit"),
                                 ("execution_datetime", "dateProcessed"),
                                 ]:
            print('#### rslt: ', rslt_nm, type(rslt[rslt_nm]), rslt[rslt_nm], 'exp.json: ', expt_nm, type(experiment_md[expt_nm]), experiment_md[expt_nm])
        print('####')


        for rslt_nm, expt_nm, tp in [("resolution_x_value", "xyResolution", float),
                                     ("resolution_y_value", "xyResolution", float),
                                     ("resolution_z_value", "zPitch", float),
                                     ("number_of_cycles", "cycle_upper_limit", int),
                                     #("execution_datetime", "dateProcessed", datetime),
                                     #("acquisition_instrument_model", "version", str),
                                 ]:
            if rslt_nm not in rslt:
                raise MetadataError("metadata is missing expected element {}".format(rslt_nm))
            if expt_nm not in experiment_md:
                raise MetadataError("experiment.json is missing expected element {}".format(expt_nm))
            if not close_enough_match(rslt[rslt_nm], experiment_md[expt_nm], tp):
                raise MetadataError("metadata field {} does not match experiment.json field {}"
                                    .format(rslt_nm, expt_nm))
                
        if 'number_of_antibodies' not in rslt:
            raise MetadataError("metadata is missing element number_of_antibodies")
        if 'channelNames' not in experiment_md or 'channelNamesArray' not in experiment_md['channelNames']:
            raise MetadataError('experiment.json is missing expected element channelNamesArray')
        arr = [elt for elt in experiment_md['channelNames']['channelNamesArray']
               if elt not in ['Blank', 'Empty'] and not elt.startswith('DAPI')]
        if not close_enough_match(rslt['number_of_antibodies'], len(arr), int):
            raise MetadataError("metadata field number_of_antibodies does not match length of"
                                "experiment.json channelNamesArray")

        return rslt
            
        
