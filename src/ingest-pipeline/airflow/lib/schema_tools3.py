from os.path import join, dirname

import fastjsonschema
import jsonref
import simplejson
from jsonschema import validate


def assert_valid_schema(data, schema_file):
    """ Checks whether the given data matches the schema """

    schema = _load_json_schema(schema_file)
    print(schema)
    data = simplejson.dumps(data)
    fastjson_result = fastjsonschema.validate(definition=schema, data=data)
    jsonschema_result = validate(instance=data, schema=schema)

    return fastjson_result, jsonschema_result


def _load_json_schema(filename):
    """ Loads the given schema file """

    relative_path = join('../../schemata', filename)
    absolute_path = join(dirname(__file__), relative_path)

    base_path = dirname(absolute_path)
    base_uri = 'file://{}/'.format(base_path)

    with open(absolute_path) as schema_file:
        file_read = schema_file.read()
        json_content = jsonref.loads(file_read, base_uri=base_uri, jsonschema=True)
        return json_content


sample_json = {
    'files': [
        {'rel_path': './trig_rnaseq_10x.py', 'type': 'unknown', 'size': 2198,
         'sha1sum': '8cbba27b76806091ec1041bc7994dfc89c60a4e2'},
        {'rel_path': './utils.py', 'type': 'unknown', 'size': 5403,
         'sha1sum': 'd910cf4a1d2b6ef928b449b906d79cab5dad1692'},
        {'rel_path': './scan_and_begin_processing.py', 'type': 'unknown', 'size': 6977,
         'sha1sum': 'c5b981ec9ddb922c84ba67127485cfa6819f79da'},
        {'rel_path': './mock_ingest_vanderbilt.py', 'type': 'unknown', 'size': 3477,
         'sha1sum': 'bf6fbb87e4dc1425525f91ce4c2238a2cc851d01'},
        {'rel_path': './mock_ingest_rnaseq_10x.py', 'type': 'unknown', 'size': 3654,
         'sha1sum': '93f204cf3878e3095a83651d2046d5393008844c'}
    ]
}

assert_valid_schema(data=sample_json, schema_file="dataset_metadata_schema.json")
