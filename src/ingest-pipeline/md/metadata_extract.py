#! /usr/bin/env python

import sys
import os
import argparse
import json
import yaml

from type_base import MetadataError
from data_collection import DataCollection
import data_collection_types
from hubmap_commons.schema_tools import assert_json_matches_schema, set_schema_base_path

_KNOWN_DATA_COLLECTION_TYPES = None

DEFAULT_SCHEMA = 'datacollection_metadata_schema.yml'
SCHEMA_BASE_PATH = os.path.join(os.path.dirname(os.path.realpath(os.path.dirname(__file__))),
                                'schemata')
SCHEMA_BASE_URI = 'http://schemata.hubmapconsortium.org/'
set_schema_base_path(SCHEMA_BASE_PATH, SCHEMA_BASE_URI)


def scan(target_dir, out_fname, schema_fname, yaml_flag=False):
    global _KNOWN_DATA_COLLECTION_TYPES

    if _KNOWN_DATA_COLLECTION_TYPES is None:
        lst = []
        for nm in dir(data_collection_types):
            elt = getattr(data_collection_types, nm)
            if isinstance(elt, type) and issubclass(elt, DataCollection):
                lst.append(elt)
        _KNOWN_DATA_COLLECTION_TYPES = lst

    for collection_type in _KNOWN_DATA_COLLECTION_TYPES:
        if collection_type.test_match(target_dir):
            #print('collector match: ', collection_type.category_name)
            collector = collection_type(target_dir)
            metadata = collector.filter_metadata(collector.collect_metadata())
            #print('collector: ', repr(collector))
            #print('metadata: %s' % metadata)
            break
    else:
        raise MetadataError('%s does not match any known data collection type'
                            % target_dir)
    assert_json_matches_schema(metadata, schema_fname)
    if yaml_flag:
        with sys.stdout if out_fname is None else open(out_fname, 'w') as f:
            yaml.dump(metadata, f)
    else:
        with sys.stdout if out_fname is None else open(out_fname, 'w') as f:
            json.dump(metadata, f)
        

def main(myargv=None):
    if myargv is None:
        myargv = sys.argv

    #default_schema_path = os.path.join(os.path.dirname(__file__), '../schemata/', DEFAULT_SCHEMA)
    default_schema_path = DEFAULT_SCHEMA  # trust the schema tools to know where to look

    parser = argparse.ArgumentParser(description='Scan a directory tree of data'
                                     ' files and extract metadata');
    parser.add_argument('--out', default=None,
                        help='Full pathname of output JSON (defaults to stdout)')
    parser.add_argument('--schema', default=None, nargs=1,
                        help=('Schema against which the output will be checked'
                              ' (default %s)' % default_schema_path))
    parser.add_argument('dir', default=None, nargs='?',
                        help='directory to scan (defaults to CWD)')
    parser.add_argument('--yaml', default=False, action='store_true')
    ns = parser.parse_args(myargv[1:])

    schema_fname = default_schema_path if ns.schema is None else ns.schema
    out_fname = ns.out
    target_dir = (os.getcwd() if ns.dir is None else ns.dir)
    yaml_flag = ns.yaml
    scan(target_dir=target_dir, out_fname=out_fname, schema_fname=schema_fname, yaml_flag=yaml_flag)
    

if __name__ == '__main__':
    main()