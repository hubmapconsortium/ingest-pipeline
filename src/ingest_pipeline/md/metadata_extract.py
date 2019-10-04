#! /usr/bin/env python

import sys
import os
import argparse
import json

from data_collection import DataCollection
from ims_data_collection import IMSDataCollection

DEFAULT_SCHEMA = 'metadata_schema.yml'

known_data_collection_types = [DataCollection, IMSDataCollection]


class MetadataError(RuntimeError):
    pass


def check_schema(jsn, schema_fname):
    """
    Check the given json data against the jsonschema in the given schema file,
    raising an exception on error.
    """
    pass
    

def scan(target_dir, out_fname, schema_fname):

    for collection_type in known_data_collection_types:
        if collection_type.test_match(target_dir):
            print('collector match: ', collection_type.category_name)
            collector = collection_type(target_dir)
            metadata = collector.collect_metadata()
            print('collector: ', repr(collector))
            print('metadata: %s' % metadata)
            break
    else:
        raise MetadataError('%s does not match any known data collection type'
                            % target_dir)
    check_schema(metadata, schema_fname)
    if out_fname is None:
        json.dump(metadata, sys.stdout)
    else:
        with open(out_fname, 'w') as f:
            json.dump(metadata, f)
        

def main(myargv=None):
    if myargv is None:
        myargv = sys.argv

    default_schema_path = os.path.join(os.path.dirname(__file__), '../schemata/', DEFAULT_SCHEMA)

    parser = argparse.ArgumentParser(description='Scan a directory tree of data'
                                     ' files and extract metadata');
    parser.add_argument('--out', default=None,
                        help='Full pathname of output JSON (defaults to stdout)')
    parser.add_argument('--schema', default=None, nargs=1,
                        help=('Schema against which the output will be checked'
                              ' (default %s)' % default_schema_path))
    parser.add_argument('dir', default=None, nargs='?',
                        help='directory to scan (defaults to CWD)')
    ns = parser.parse_args(myargv[1:])

    schema_fname = default_schema_path if ns.schema is None else ns.schema
    out_fname = ns.out
    target_dir = (os.getcwd() if ns.dir is None else ns.dir)
    scan(target_dir=target_dir, out_fname=out_fname, schema_fname=schema_fname)
    

if __name__ == '__main__':
    main()