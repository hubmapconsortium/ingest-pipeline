#! /usr/bin/env python

import sys
import os
from pathlib import Path
from urllib.parse import urlparse, urlsplit
from urllib.request import urlopen
import json
import yaml

import jsonschema

DEFAULT_SCHEMA = 'metadata_schema.yml'

_SCHEMA_BASE_PATH = os.path.join(Path(__file__).parent.parent, 'schemata')

class SchemaError(RuntimeError):
    pass


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


def file_uri_handler(uri):
    p = urlsplit(uri)
    assert p.scheme == 'file', 'This handler only takes file URIs; got %s' % uri
    ext = os.path.splitext(p.path)[1]
    if ext in ['.yaml', '.yml']:
        with open(p.path, "rU") as f:
            result = yaml.safe_load(f)
    elif ext in ['.jsn', '.json']:
        result = json.loads(urlopen(uri).read().decode("utf-8"))
    else:
        raise SchemaError('Unrecognized extension in the file URI %s' % uri)
    return result


def set_schema_base_path(base_path):
    global _SCHEMA_BASE_PATH
    _SCHEMA_BASE_PATH = os.path.abspath(base_path)


def get_validator(schema_uri):
    p = urlsplit(schema_uri)
    if p.scheme == '':
        # blank filename
        if p.path.startswith('/'):
            base_path = os.path.dirname(os.path.abspath(p.path))
            schema = file_to_json(p.path)
        elif _SCHEMA_BASE_PATH is None:
            base_path = os.path.dirname(os.path.abspath(p.path))
            schema = file_to_json(p.path)
        else:
            base_path = _SCHEMA_BASE_PATH
            schema = file_to_json(os.path.join(base_path, p.path))
        resolver = jsonschema.RefResolver('file://' + base_path + '/', schema,
                                          handlers={'file': file_uri_handler})
    else:
        schema = json.loads(urlopen(schema_uri).read().decode("utf-8"))
        resolver = jsonschema.RefResolver(schema_uri, None,
                                          handlers={'file': file_uri_handler})
    validator = jsonschema.validators.validator_for(schema)(schema=schema,
                                                            resolver=resolver)
    return validator


def check_schema(jsn, schema_fname):
    """
    Check the given json data against the jsonschema in the given schema file,
    raising an exception on error.
    """
    validator = get_validator(schema_fname)
    err_msg_l = []
    try:
        for error in validator.iter_errors(jsn):
            err_msg_l.append('{}: {}'.format(' '.join([str(word) for word in error.path]), error.message))
        if err_msg_l:
            raise SchemaError( ' + '.join(err_msg_l))
    except Exception as e:
        raise SchemaError('{}'.format(e))

    

