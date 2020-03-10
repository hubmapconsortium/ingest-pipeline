from os.path import join, dirname, abspath, splitext

from urllib import parse as urlparse
from urllib.request import urlopen
import requests

import fastjsonschema
import jsonref
import json
import yaml
import simplejson
from jsonschema import validate
from jsonschema.exceptions import ValidationError, SchemaError


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


def assert_valid_schema(data, schema_file):
    """ Checks whether the given data matches the schema """

    schema = _load_json_schema(schema_file)
    print('FINAL SCHEMA FOLLOWS')
    print(schema)
    print('END FINAL SCHEMA')
    try:
        fastjson_compiled_schema = fastjsonschema.compile(schema)
        print('FASTJSONSCHEMA COMPILATION SUCCEEDED')
        fastjson_compiled_schema(data)
        print('FASTJSONSCHEMA VALIDATION SUCCEEDED')
    except fastjsonschema.JsonSchemaDefinitionException as e:
        print('FASTJSONSCHEMA COMPILATION FAILED')
        fastjson_result = False
    except fastjsonschema.JsonSchemaException as e:
        print('FASTJSON VALIDATION FAILED')
        fastjson_result = False
    else:
        fastjson_result = True
    print('FASTJSON says ', fastjson_result)
    try:
        validate(instance=data, schema=schema)
    except SchemaError as e:
        print('schema was invalid: {}'.format(e))
        jsonschema_result = False
    except ValidationError as e:
        print('validation failed: {}'.format(e))
        jsonschema_result = False
    else:
        jsonschema_result = True
    print('JSONSCHEMA says ', jsonschema_result)

    return fastjson_result, jsonschema_result


def _load_json_schema(filename):
    """ Loads the given schema file """

    relative_path = join('../../schemata', filename)
    absolute_path = abspath(join(dirname(__file__), relative_path))

    base_path = dirname(absolute_path)

    loader = LocalJsonLoader(base_path)
    src_uri = 'file:///{}'.format(filename)
    print('SRC_URI: ', src_uri)
    base_uri = 'http://schemata.hubmapconsortium.org/{}'.format(filename)
    print('BASE_URI: ', base_uri)
    json_content = jsonref.load_uri(src_uri, base_uri=base_uri, loader=loader,
                                    jsonschema=True, load_on_repr=False)

    return json_content


sample_json = {
    'files': [
        {'rel_path': './trig_rnaseq_10x.py', 'filetype': 'unknown', 'size': 2198,
         'sha1sum': '8cbba27b76806091ec1041bc7994dfc89c60a4e2'},
        {'rel_path': './utils.py', 'type': 'unknown', 'size': 5403,
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

assert_valid_schema(data=sample_json, schema_file="dataset_metadata_schema.json")
