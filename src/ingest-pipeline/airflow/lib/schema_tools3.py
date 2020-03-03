from os.path import join, dirname, abspath

from urllib import parse as urlparse

import fastjsonschema
import jsonref
import json
import simplejson
from jsonschema import validate
from jsonschema.exceptions import ValidationError, SchemaError


class LocalJsonLoader(jsonref.JsonLoader):
    def __init__(self, schema_root_dir, **kwargs):
        super(LocalJsonLoader, self).__init__(**kwargs)
        self.schema_root_dir = schema_root_dir

    def patch_uri(self, uri):
        surl = urlparse.urlsplit(uri)
        if surl.scheme == 'file' and not surl.path.startswith(self.schema_root_dir):
            assert surl.path[0] == '/', 'problem parsing path component of a file uri'
            puri = urlparse.urlunsplit((surl.scheme, surl.netloc,
                                        join(self.schema_root_dir, surl.path[1:]),
                                        surl.query, surl.fragment))
        else:
            puri = uri
        return puri

    def __call__(self, uri, **kwargs):
        return super(LocalJsonLoader, self).__call__(self.patch_uri(uri), **kwargs)
    
    def get_remote_json(self, uri, **kwargs):
        return super(LocalJsonLoader, self).get_remote_json(self.patch_uri(uri), **kwargs)


def assert_valid_schema(data, schema_file):
    """ Checks whether the given data matches the schema """

    schema = _load_json_schema(schema_file)
    schema = dict(schema) # because it's not really a dict 
    print('FINAL SCHEMA FOLLOWS')
    print(schema)
    print('END FINAL SCHEMA')
#     fastjson_compiled_schema = fastjsonschema.compile(schema)
#     print('COMPILATION SUCCEEDED')
#     fastjson_result = fastjson_compiled_schema(data)
    fastjson_result = True
    print('FASTJSON says ', fastjson_result)
    try:
        validate(instance=data, schema=schema)
    except SchemaError:
        print('schema was invalid')
        jsonschema_result = False
    except ValidationError:
        print('validation failed')
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
    #base_uri = 'file://{}/'.format(base_path)
    #base_uri = 'file:////{}'.format(absolute_path)
    #base_uri = 'file://{}'.format(absolute_path)

    loader = LocalJsonLoader(base_path)
    base_uri = 'file:///{}'.format(filename)
    print('BASE_URI: ', base_uri)
    json_content = jsonref.load_uri(base_uri, loader=loader,
                                    jsonschema=True, load_on_repr=False)

#     with open(absolute_path) as schema_file:
#         file_read = schema_file.read()
#         print('BEGIN READ')
#         print(file_read)
#         print('END READ')
#         #json_content = jsonref.loads(file_read, base_uri=base_uri, jsonschema=True)
#         json_content = jsonref.load(absolute_path, base_uri=base_uri, jsonschema=True)
#         return json_content

#     with open(absolute_path) as schema_file:
#         #json_content = jsonref.loads(file_read, base_uri=base_uri, jsonschema=True)
#         loader = LocalJsonLoader(base_path)
#         json_content = jsonref.load(schema_file, loader=loader, base_uri=base_uri,
#                                     jsonschema=True, load_on_repr=False)
#         #base_uri=base_uri, jsonschema=True, load_on_repr=False)

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
    ]
}

assert_valid_schema(data=sample_json, schema_file="dataset_metadata_schema.json")
