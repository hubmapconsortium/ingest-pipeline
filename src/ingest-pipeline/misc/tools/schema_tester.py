#! /usr/bin/env python

#
# This is a heavily modified version of the schema_tester.py routine from
# pyRHEA, https://github.com/PSC-PublicHealth/pyrhea

import sys
from os import walk
from os.path import (
    dirname, join, realpath, isdir
)
import optparse
import yaml
import json
from typing import (
    Any, Dict, List, Union
)

from jsonschema.exceptions import (
    ValidationError,
    SchemaError,
    UnknownType,
    UndefinedTypeCheck
)
from hubmap_commons.schema_tools import (
    check_json_matches_schema
)

JSONType = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]

SCHEMA_BASE_PATH = join(dirname(dirname(dirname(realpath(__file__)))),
                        'schemata')
SCHEMA_BASE_URI = 'http://schemata.hubmapconsortium.org/'


def file_to_json(fname):
    if fname.lower().endswith('.json') or fname.lower().endswith('.jsn'):
        with open(fname, "r") as f:
            tjson = json.load(f)
    else:
        assert fname.lower().endswith('.yaml') or fname.lower().endswith('.yml'), \
            "File type of %s is not understood" % fname
        with open(fname, "r") as f:
            tjson = yaml.safe_load(f)
    return tjson


def main(myargv=None):
    "Provides a few test routines"

    if myargv is None:
        myargv = sys.argv

    parser = optparse.OptionParser(usage="""
    %prog [-R] schemaFile checkFile

    The schema and the file(s) to be checked can be JSON or YAML.  schemaFile is just a
    filename, as the path to the schema directory is known.
    """)

    parser.add_option("-R", action="store_true",
                      help="If checkFile is a directory, check files recursively")

    opts, args = parser.parse_args()

    if len(args) == 2:
        schema_fname = args[0]

        if opts.R and isdir(args[1]):
            for root, _, files in walk(args[1]):
                for fName in files:
                    if fName.lower().endswith(('.yaml', '.yml', '.json', '.jsn')):
                        path = join(root, fName)
                        try:
                            jsn = file_to_json(path)
                            check_json_matches_schema(jsn, schema_fname, SCHEMA_BASE_PATH, SCHEMA_BASE_URI)
                            print(f'PASS: {path}')
                        except ValidationError as excp:
                            print(f'FAILED: {path}: {excp}')
                        except (SchemaError, UnknownType, UndefinedTypeCheck) as excp:
                            print(f'Schema is invalid: {path} {excp}')
        else:
            try:
                jsn = file_to_json(args[1])
                check_json_matches_schema(jsn, schema_fname, SCHEMA_BASE_PATH, SCHEMA_BASE_URI)
                print(f'PASS: {args[1]}')
            except ValidationError as excp:
                print(f'FAILED: {args[1]}: {excp}')
            except (SchemaError, UnknownType, UndefinedTypeCheck) as excp:
                print(f'SCHEMA IS INVALID: {args[0]} {excp}')
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
