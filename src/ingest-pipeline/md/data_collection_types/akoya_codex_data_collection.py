#! /usr/bin/env python

import os
import json
import glob
from datetime import datetime, timedelta
import pytz
import re
from pprint import pprint

from type_base import MetadataError
from data_collection import DataCollection


def translate_timestamp(tstr, default_tz):
    try:
        d = datetime.strptime(tstr, '%Y-%m-%dT%H:%M:%S.%f%z')
        return d
    except ValueError:
        try:
            d = datetime.strptime(tstr, '%Y-%m-%d %H:%M:%S.%f')
            d = default_tz.localize(d)
            return d
        except:
            try:
                d = datetime.strptime(tstr, '%Y-%m-%d %H:%M:%S')
                d = default_tz.localize(d)
                return d
            except:
                try:
                    d = datetime.strptime(tstr, '%Y-%m-%d %H:%M')
                    d = default_tz.localize(d)
                    return d
                except:
                    raise MetadataError('Cannot translate time string {}'.format(tstr))


def close_enough_match(v1, v2, tp):
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
        d1 = translate_timestamp(v1, timezone)
        d2 = translate_timestamp(v2, timezone)
        dlt = d1 - d2
        #print('timedelta: ', dlt)
        return abs(dlt).total_seconds() <= 3600.0  # We will allow up to an hour delta
    else:
        raise MetadataError('close_enough_match does not know how to compare values of type {}'
                            .format(tp.__name__))


class AkoyaCODEXDataCollection(DataCollection):
    category_name = 'AKOYA_CODEX';
    top_target = 'experiment.json'
    dir_regex = re.compile('src_.*')

    # expected_file pairs are (globable name, filetype key)
    expected_files = [('*-metadata.tsv', 'METADATATSV'),
                      ('{offsetdir}/experiment.json', "JSON"),
                      ('{offsetdir}/segmentation.json', "JSON")
                      ]
    
    optional_files = [('exposure_times.txt', 'CSV')]

    @classmethod
    def find_top(cls, path, target, dir_regex=None):
        """
        In some cases, all or most of the needed files are in a subdirectory.  If it
        exists, find it.
        """
        if target in os.listdir(path):
            return '.'
        else:
            if dir_regex is None:
                candidates = [nm for nm in os.listdir(path)
                              if os.path.isdir(os.path.join(path, nm))]
            else:
                candidates = [nm for nm in os.listdir(path)
                              if os.path.isdir(os.path.join(path, nm)) and dir_regex.match(nm)]
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
        offsetdir = cls.find_top(path, cls.top_target, cls.dir_regex)
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
        self.offsetdir = self.find_top(self.topdir, self.top_target, self.dir_regex)
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


    def basic_filter_metadata(self, raw_metadata):
        """
        Make sure basic components of metadata are present, and promote them
        """
        rslt = {k : raw_metadata[k] for k in ['collectiontype']}
        if len(raw_metadata['components']) != 1:
            raise MetadataError("Only one line of metadata.tsv info is currently supported")
        rslt.update(raw_metadata['components'][0])
        
        # Hard-coded reality checks
        if 'assay_type' not in rslt or rslt['assay_type'] != 'CODEX':
            raise MetadataError('assay_type is not CODEX')

        return rslt


    def internal_consistency_checks(self, rslt, experiment_md, test_tpl_lst, channel_names):
        """
        Check a variety of relationships that are believed to hold between [Ee]xperiment.json and
        metadata found in the metadata.tsv file.
        """
        for rslt_nm, expt_nm, _ in test_tpl_lst:
            try:
                print('#### rslt: ', rslt_nm, type(rslt[rslt_nm]), rslt[rslt_nm], 'exp.json: ', expt_nm, type(experiment_md[expt_nm]), experiment_md[expt_nm])
            except KeyError as e:
                print('#### rslt: ', rslt_nm, type(rslt[rslt_nm]), rslt[rslt_nm], 'KeyError: ', e)
        print('####')


        for rslt_nm, expt_nm, tp in test_tpl_lst:
            if rslt_nm not in rslt:
                raise MetadataError("metadata is missing expected element {}".format(rslt_nm))
            if expt_nm not in experiment_md:
                raise MetadataError("experiment.json is missing expected element {}".format(expt_nm))
            if not close_enough_match(rslt[rslt_nm], experiment_md[expt_nm], tp):
                raise MetadataError("metadata field {} does not match experiment.json field {}"
                                    .format(rslt_nm, expt_nm))
                
        if 'number_of_antibodies' not in rslt:
            raise MetadataError("metadata is missing element number_of_antibodies")
        if channel_names is not None:
            arr = [elt for elt in channel_names
                   if elt.lower() not in ['blank', 'empty'] and not elt.startswith('DAPI') and not elt.startswith('HOECHST')]
            #print(channel_names)
            if not close_enough_match(rslt['number_of_antibodies'], len(arr), int):
                raise MetadataError("metadata field number_of_antibodies does not match length of"
                                    "experiment.json channelNamesArray")


    def filter_metadata(self, raw_metadata):
        """
        This extracts the metadata which is actually desired downstream from the bulk of the
        metadata which has been collected.
        
        """
        rslt = self.basic_filter_metadata(raw_metadata)

        candidate_l = [raw_metadata[k] for k in raw_metadata if 'experiment.json' in k] 
        if not candidate_l:
            raise MetadataError('Cannot find experiment.json')
        
        experiment_md = candidate_l[0]
        for elt in candidate_l[1:]:
            if elt != experiment_md:
                raise MetadataError("Multiple experiment.json files that do not match")

        tests = [("resolution_x_value", "xyResolution", float),
                 ("resolution_y_value", "xyResolution", float),
                 ("resolution_z_value", "zPitch", float),
                 ("number_of_cycles", "cycle_upper_limit", int),
                 ("execution_datetime", "dateProcessed", datetime),
                 #("acquisition_instrument_model", "version", str),
                 ]
        if 'channelNames' not in experiment_md or 'channelNamesArray' not in experiment_md['channelNames']:
            raise MetadataError('experiment.json is missing expected element channelNamesArray')
        self.internal_consistency_checks(rslt, experiment_md, tests, experiment_md['channelNames']['channelNamesArray'])

        return rslt
            
        
