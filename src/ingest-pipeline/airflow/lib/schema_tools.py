#! /usr/bin/env python

import sys
import os
from os.path import join, dirname, abspath, splitext
from pathlib import Path
from urllib.parse import urlparse, urlsplit
from urllib.request import urlopen
from urllib import parse as urlparse
from urllib.request import urlopen
import requests
import json
import yaml
from jsonschema import validate
from jsonschema.validators import Draft7Validator as Validator
from jsonschema.exceptions import ValidationError, SchemaError
import jsonref


_SCHEMA_BASE_PATH = str(Path(__file__).resolve().parent.parent.parent / 'schemata')

_SCHEMA_BASE_URI = 'http://schemata.hubmapconsortium.org/'


def set_schema_base_path(base_path):
    global _SCHEMA_BASE_PATH
    _SCHEMA_BASE_PATH = os.path.abspath(base_path)


class LocalJsonLoader(jsonref.JsonLoader):
    def __init__(self, schema_root_dir, **kwargs):
        super(LocalJsonLoader, self).__init__(**kwargs)
        self.schema_root_dir = schema_root_dir
        self.schema_root_uri = None # the name by which the root doc knows itself

    def patch_uri(self, uri):
        suri = urlparse.urlsplit(uri)
        if self.schema_root_uri is not None:
            root_suri = urlparse.urlsplit(self.schema_root_uri)
            if suri.scheme == root_suri.scheme and suri.netloc == root_suri.netloc:
                # This file is actually local
                suri = suri._replace(scheme='file', netloc='')
        if suri.scheme == 'file' and not suri.path.startswith(self.schema_root_dir):
            assert suri.path[0] == '/', 'problem parsing path component of a file uri'
            puri = urlparse.urlunsplit((suri.scheme, suri.netloc,
                                        join(self.schema_root_dir, suri.path[1:]),
                                        suri.query, suri.fragment))
        else:
            puri = urlparse.urlunsplit(suri)
        return puri

    def __call__(self, uri, **kwargs):
        rslt = super(LocalJsonLoader, self).__call__(uri, **kwargs)
        if self.schema_root_uri is None and '$id' in rslt:
            self.schema_root_uri = rslt['$id']
            if uri in self.store:
                self.store[self.schema_root_uri] = self.store[uri]
        return rslt
    
    def get_remote_json(self, uri, **kwargs):
        uri = self.patch_uri(uri)
        puri = urlparse.urlsplit(uri)
        scheme = puri.scheme
        ext = splitext(puri.path)[1]
        other_kwargs = {k:v for k, v in kwargs.items() if k not in ['base_uri', 'jsonschema']}

        if scheme in ["http", "https"]:
            # Prefer requests, it has better encoding detection
            result = requests.get(uri).json(**kwargs)
        else:
            # Otherwise, pass off to urllib and assume utf-8
            if ext in ['.yml', '.yaml']:
                result = yaml.load(urlopen(uri).read().decode("utf-8"), **other_kwargs)
            else:
                result = json.loads(urlopen(uri).read().decode("utf-8"), **other_kwargs)

        return result


def _load_json_schema(filename):
    """
    Loads the schema file of the given name.
    
    The filename is relative to the root schema directory.
    JSON and YAML formats are supported.
    """
    loader = LocalJsonLoader(_SCHEMA_BASE_PATH)
    src_uri = 'file:///{}'.format(filename)
    base_uri = '{}{}'.format(_SCHEMA_BASE_URI, filename)
    return jsonref.load_uri(src_uri, base_uri=base_uri, loader=loader,
                            jsonschema=True, load_on_repr=False)


def assert_json_matches_schema(jsondata, schema_filename):
    """
    raises AssertionError if the schema in schema_filename
    is invalid, or if the given jsondata does not match the schema.
    """
    schema = _load_json_schema(schema_filename)
    #print('FINAL SCHEMA FOLLOWS')
    #print(schema)
    #print('END FINAL SCHEMA')
    try:
        validate(instance=jsondata, schema=schema)
    except SchemaError as e:
        raise AssertionError('{} is an invalid schema: {}'.format(schema_filename, e))
    except ValidationError as e:
        raise AssertionError('json does not match {}: {}'.format(schema_filename, e))


def check_json_matches_schema(jsondata, schema_filename):
    """
    Check the given json data against the jsonschema in the given schema file,
    raising an exception on error.  The exception text includes one or more
    validation error messages.
    
    schema_filename is relative to the schema root directory.
    
    may raise SchemaError or ValidationError
    """
    try:
        validator = Validator(_load_json_schema(schema_filename))
    except SchemaError as e:
        raise SchemaError('{} is invalid: {}'.format(schema_filename, e))
    err_msg_l = []
    for error in validator.iter_errors(jsondata):
        err_msg_l.append('{}: {}'.format(' '.join([str(word) for word in error.path]), error.message))
    if err_msg_l:
        raise ValidationError( ' + '.join(err_msg_l))

    
def main():
    sample_json = {
        'files': [
            {'rel_path': './trig_rnaseq_10x.py', 'type': 'unknown', 'size': 2198, 
             'sha1sum': '8cbba27b76806091ec1041bc7994dfc89c60a4e2'},
            {'rel_path': './utils.py', 'filetype': 'unknown', 'size': 5403,
             'sha1sum': 'd910cf4a1d2b6ef928b449b906d79cab5dad1692'},
            {'rel_path': './scan_and_begin_processing.py', 'filetype': 'unknown', 'size': 6977,
             'sha1sum': 'c5b981ec9ddb922c84ba67127485cfa6819f79da'},
            {'rel_path': './mock_ingest_vanderbilt.py', 'filetype': 'unknown', 'size': 3477,
             'sha1sum': 'bf6fbb87e4dc1425525f91ce4c2238a2cc851d01'},
            {'rel_path': './mock_ingest_rnaseq_10x.py', 'filetype': 'unknown', 'size': 3654,
             'sha1sum': '93f204cf3878e3095a83651d2046d5393008844c'}
            ],
            'dag_provenance': {'trig_codex.py': '0123456789abcdefABCDEF'}
        }
    bad_json = {
        'files': [
            {'rel_path': './trig_rnaseq_10x.py', 'type': 'unknown', 'size': 2198, 
             'sha1sum': '8cbba27b76806091ec1041bc7994dfc89c60a4e2'},
            {'rel_path': './utils.py', 'filetype': 'unknown', 'size': 5403,
             'sha1sum': 'd910cf4a1d2b6ef928b449b906d79cab5dad1692'},
            {'rel_path': './scan_and_begin_processing.py', 'filetype': 'unknown', 'size': 6977,
             'sha1sum': 'c5b981ec9ddb922c84ba67127485cfa6819f79da'},
            {'rel_path': './mock_ingest_vanderbilt.py', 'filetype': 'dubious', 'size': 3477,
             'sha1sum': 'bf6fbb87e4dc1425525f91ce4c2238a2cc851d01'},
            {'rel_path': './mock_ingest_rnaseq_10x.py', 'filetype': 'unknown', 'size': 3654,
             'sha1sum': '93f204cf3878e3095a83651d2046d5393008844'}
            ],
            'dag_provenance': {'trig_codex.py': '0123456789abcdefABCDEFG'}
        }

    for lbl, jsondata in [('correct', sample_json), ('incorrect', bad_json)]:
        try:
            assert_json_matches_schema(jsondata, 'dataset_metadata_schema.yml')
            print('assertion passed for {}'.format(lbl))
        except AssertionError as e:
            print('assertion failed for {}: {}'.format(lbl, e))
            
        try:
            check_json_matches_schema(jsondata, 'dataset_metadata_schema.json')
            print('check passed for {}'.format(lbl))
        except (SchemaError, ValidationError) as e:
            print('check failed for {}: {}'.format(lbl, e))
 
 
if __name__ == "__main__":
    main()


