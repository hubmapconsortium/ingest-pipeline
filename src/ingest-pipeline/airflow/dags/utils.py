from os import environ, walk
from os.path import basename, dirname, relpath, split, join, getsize, realpath
import sys
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Pattern, Tuple, TypeVar, Union
from subprocess import check_call, check_output, CalledProcessError
import re
import json
from pprint import pprint
import uuid
import yaml

from airflow.configuration import conf as airflow_conf

from hubmap_commons.schema_tools import assert_json_matches_schema, set_schema_base_path

JSONType = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]

# Some functions accept a `str` or `List[str]` and return that same type
StrOrListStr = TypeVar('StrOrListStr', str, List[str])

SCHEMA_BASE_PATH = join(dirname(dirname(dirname(realpath(__file__)))),
                        'schemata')
SCHEMA_BASE_URI = 'http://schemata.hubmapconsortium.org/'

from airflow.hooks.http_hook import HttpHook

# Some constants
PIPELINE_BASE_DIR = Path(environ['AIRFLOW_HOME']) / 'pipeline_git_repos'

# default maximum for number of files for which info should be returned in_line
# rather than via an alternative scratch file
MAX_IN_LINE_FILES = 500

GIT = 'git'
GIT_CLONE_COMMAND = [
    GIT,
    'clone',
    '{repository}',
]
GIT_FETCH_COMMAND = [
    GIT,
    'fetch',
]
GIT_CHECKOUT_COMMAND = [
    GIT,
    'checkout',
    '{ref}',
]
GIT_LOG_COMMAND = [
    GIT,
    'log',
    '-n1',
    '--oneline'
]
GIT_ORIGIN_COMMAND = [
    GIT,
    'config',
    '--get',
    'remote.origin.url'
]
GIT_ROOT_COMMAND = [
    GIT,
    'rev-parse',
    '--show-toplevel'
]
SHA1SUM_COMMAND = [
    'sha1sum',
    '{fname}'
]
FILE_TYPE_MATCHERS = [(r'^.*\.csv$', 'csv'),  # format is (regex, type)
                      (r'^.*\.hdf5$', 'hdf5'),
                      (r'^.*\.h5ad$', 'h5ad'),
                      (r'^.*\.pdf$', 'pdf'),
                      (r'^.*\.json$', 'json'),
                      (r'^.*\.arrow$', 'arrow'),
                      (r'(^.*\.fastq$)|(^.*\.fastq.gz$)', 'fastq'),
                      (r'(^.*\.yml$)|(^.*\.yaml$)', 'yaml')
                      ]
COMPILED_TYPE_MATCHERS: Optional[List[Tuple[Pattern, str]]] = None

"""
Lazy construction; a list of tuples (collection_type_regex, assay_type_regex, workflow)
"""
WORKFLOW_MAP_FILENAME = 'workflow_map.yml'  # Expected to be found in the same dir as this file
WORKFLOW_MAP_SCHEMA = 'workflow_map_schema.yml'
COMPILED_WORKFLOW_MAP: Optional[List[Tuple[Pattern, Pattern, str]]] = None


def clone_or_update_pipeline(pipeline_name: str, ref: str = 'origin/master') -> None:
    """
    Ensure that a Git clone of a specific pipeline exists inside the
    PIPELINE_BASE_DIR directory.

    If it doesn't exist already, clone it and check out the specified ref.
    If it already exists, run 'git fetch' inside, then check out the specified ref.
    With the default ref (origin/master), this will mimic running 'git pull'.

    :param pipeline_name: the name of a public repository on GitHub, under the
      'hubmapconsortium' organization. The remote repository URL will be constructed
      as 'https://github.com/hubmapconsortium/{pipeline_name}'.
    :param ref: which reference to check out in the repository after cloning
      or fetching. This can be a remote branch (prefixed with 'origin/') or a
      tag if a pipeline should be pinned to a specific version.
    """
    PIPELINE_BASE_DIR.mkdir(parents=True, exist_ok=True)
    pipeline_dir = PIPELINE_BASE_DIR / pipeline_name
    if pipeline_dir.is_dir():
        # Already exists. Fetch and update
        check_call(GIT_FETCH_COMMAND, cwd=pipeline_dir)
    else:
        repository_url = f'https://github.com/hubmapconsortium/{pipeline_name}'
        clone_command = [
            piece.format(repository=repository_url)
            for piece in GIT_CLONE_COMMAND
        ]
        check_call(clone_command, cwd=PIPELINE_BASE_DIR)

    # Whether we just cloned the repository or fetched any updates that may
    # exist, check out the ref given as the argument to this function.
    # This is required even when cloning, since you can't do (e.g.)
    #   `git clone -b origin/master whatever-repo`

    checkout_command = [
        piece.format(ref=ref)
        for piece in GIT_CHECKOUT_COMMAND
    ]
    check_call(checkout_command, cwd=pipeline_dir)


def get_git_commits(file_list: StrOrListStr) -> StrOrListStr:
    """
    Given a list of file paths, return a list of the current short commit hashes of those files
    """
    rslt = []
    if isinstance(file_list, str):  # sadly, a str is an Iterable[str]
        file_list = [file_list]
        unroll = True
    else:
        unroll = False
    for fname in file_list:
        log_command = [piece.format(fname=fname)
                       for piece in GIT_LOG_COMMAND]
        try:
            dirnm = dirname(fname)
            if dirnm == '':
                dirnm = '.'
            line = check_output(log_command, cwd=dirnm)
        except CalledProcessError as e:
            # Git will fail if this is not running from a git repo
            line = 'DeadBeef git call failed: {}'.format(e.output)
            line = line.encode('utf-8')
        hash = line.split()[0].strip().decode('utf-8')
        rslt.append(hash)
    if unroll:
        return rslt[0]
    else:
        return rslt


def get_git_origins(file_list: StrOrListStr) -> StrOrListStr:
    """
    Given a list of file paths, return a list of the git origins of those files
    """
    rslt = []
    if isinstance(file_list, str):  # sadly, a str is an Iterable[str]
        file_list = [file_list]
        unroll = True
    else:
        unroll = False
    for fname in file_list:
        command = [piece.format(fname=fname)
                   for piece in GIT_ORIGIN_COMMAND]
        try:
            dirnm = dirname(fname)
            if dirnm == '':
                dirnm = '.'
            line = check_output(command, cwd=dirnm)
        except CalledProcessError as e:
            # Git will fail if this is not running from a git repo
            line = 'https://unknown/unknown.git git call failed: {}'.format(e.output)
            line = line.encode('utf-8')
        url = line.split()[0].strip().decode('utf-8')
        rslt.append(url)
    if unroll:
        return rslt[0]
    else:
        return rslt


def get_git_root_paths(file_list: Iterable[str]) -> Union[str, List[str]]:
    """
    Given a list of file paths, return a list of the root directories of the git
    working trees of the files.
    """
    rslt = []
    if isinstance(file_list, str):  # sadly, a str is an Iterable[str]
        file_list = [file_list]
        unroll = True
    else:
        unroll = False
    for fname in file_list:
        command = [piece.format(fname=fname)
                   for piece in GIT_ROOT_COMMAND]
        try:
            dirnm = dirname(fname)
            if dirnm == '':
                dirnm = '.'
            root_path = check_output(command, cwd=dirnm)
        except CalledProcessError as e:
            root_path = dirname(fname).encode('utf-8')
        rslt.append(root_path.strip().decode('utf-8'))
    if unroll:
        return rslt[0]
    else:
        return rslt


def get_git_provenance_dict(file_list: Iterable[str]) -> Mapping[str, str]:
    """
    Given a list of file paths, return a list of dicts of the form:
    
      [{<file base name>:<file commit hash>}, ...]
    """
    if isinstance(file_list, str):  # sadly, a str is an Iterable[str]
        file_list = [file_list]
    return {basename(fname) : get_git_commits(realpath(fname))
            for fname in file_list}


def get_git_provenance_list(file_list: Iterable[str]) -> List[Mapping[str, Any]]:
    """
    Given a list of file paths, return a list of dicts of the form:
    
      [{'name':<file base name>, 'hash':<file commit hash>, 'origin':<file git origin>},...]
    """
    if isinstance(file_list, str):  # sadly, a str is an Iterable[str]
        file_list = [file_list]
    name_l = file_list
    hash_l = [get_git_commits(realpath(fname)) for fname in file_list]
    origin_l = [get_git_origins(realpath(fname)) for fname in file_list]
    root_l = get_git_root_paths(file_list)
    rel_name_l = [relpath(name, root) for name, root in zip(name_l, root_l)]
    # Make sure each repo appears only once
    repo_d = {origin: {'name': name, 'hash': hash}
              for origin, name, hash in zip(origin_l, rel_name_l, hash_l)}
    rslt = []
    for origin in repo_d:
        dct = repo_d[origin].copy()
        dct['origin'] = origin
        if not dct['name'].endswith('cwl'):
            del dct['name']  # include explicit names for workflows only
        rslt.append(dct)
    #pprint(rslt)
    return rslt


def _get_file_type(path: str) -> str:
    """
    Given a path, guess the type of the file
    """
    global COMPILED_TYPE_MATCHERS
    if COMPILED_TYPE_MATCHERS is None:
        lst = []
        for regex, tpnm in FILE_TYPE_MATCHERS:
            lst.append((re.compile(regex), tpnm))
        COMPILED_TYPE_MATCHERS = lst
    for regex, tpnm in COMPILED_TYPE_MATCHERS:
        #print('testing ', regex, tpnm)
        if regex.match(path):
            return tpnm
    return 'unknown'
    

def get_file_metadata(root_dir: str) -> List[Mapping[str, Any]]:
    """
    Given a root directory, return a list of the form:
    
      [{'rel_path':<relative path>, 'type':<file type>, 'size':<file size>}, ...]
    
    containing an entry for every file below the given root directory:
    """
    rslt = []
    for dirpth, dirnames, fnames in walk(root_dir):
        rp = relpath(dirpth, start=root_dir)
        for fn in fnames:
            full_path = join(root_dir, rp, fn)
            sz = getsize(full_path)
            # sha1sum disabled because of run time issues on large data collections
            #line = check_output([word.format(fname=full_path)
            #                     for word in SHA1SUM_COMMAND])
            #cs = line.split()[0].strip().decode('utf-8')
            rslt.append({'rel_path': join(rp, fn),
                         'type': _get_file_type(full_path),
                         'size': getsize(join(root_dir, rp, fn)),
                         #'sha1sum': cs
                         })
    return rslt


def get_file_metadata_dict(root_dir: str, alt_file_dir: str,
                           max_in_line_files : int = MAX_IN_LINE_FILES) -> Mapping[str, Any]:
    """
    This routine returns file metadata, either directly as JSON in the form
    {'files': [{...}, {...}, ...]} with the list returned by get_file_metadata() or the form
    {'files_info_alt_path': path} where path is the path of a unique file in alt_file_dir
    relative to the WORKFLOW_SCRATCH config parameter
    """
    file_info = get_file_metadata(root_dir)
    if len(file_info) > max_in_line_files:
        localized_assert_json_matches_schema(file_info, 'file_info_schema.yml')
        fpath = join(alt_file_dir, '{}.json'.format(uuid.uuid4()))
        with open(fpath, 'w') as f:
            json.dump({'files': file_info}, f)
        return {'files_info_alt_path' : relpath(fpath, _get_scratch_base_path())}
    else:
        return {'files' : file_info}

def pythonop_trigger_target(**kwargs) -> None:
    """
    When used as the python_callable of a PythonOperator,this just logs
    data provided to the running DAG.
    """
    ctx = kwargs['dag_run'].conf
    run_id = kwargs['run_id']
    print('run_id: ', run_id)
    print('dag_run.conf:')
    pprint(ctx)
    print('kwargs:')
    pprint(kwargs)
 

def pythonop_maybe_keep(**kwargs) -> None:
    """
    accepts the following via the caller's op_kwargs:
    'next_op': the operator to call on success
    'bail_op': the operator to which to bail on failure (default 'no_keep')
    'test_op': the operator providing the success code
    'test_key': xcom key to test.  Defaults to None for return code
    """
    bail_op = kwargs['bail_op'] if 'bail_op' in kwargs else 'no_keep'
    test_op = kwargs['test_op']
    test_key = kwargs['test_key'] if 'test_key' in kwargs else None
    retcode = int(kwargs['ti'].xcom_pull(task_ids=test_op, key=test_key))
    print('%s key %s: %s\n' % (test_op, test_key, retcode))
    if retcode is 0:
        return kwargs['next_op']
    else:
        return bail_op


def pythonop_send_create_dataset(**kwargs) -> str:
    """
    Requests creation of a new dataset.  Returns dataset info via XCOM
    
    Accepts the following via the caller's op_kwargs:
    'http_conn_id' : the http connection to be used
    'endpoint' : the REST endpoint
    'parent_dataset_uuid_callable' : called with **kwargs; returns uuid
                                     of the parent of the new dataset
    'dataset_name_callable' : called with **kwargs; returns the
                              display name of the new dataset                                     
    'dataset_types' : the types list of the new dataset
    
    Returns the following via XCOM:
    (no key) : data_directory_path for the new dataset
    'derived_dataset_uuid' : uuid for the created dataset
    'group_uuid' : group uuid for the created dataset
    """
    for arg in ['parent_dataset_uuid_callable', 'http_conn_id', 'endpoint',
                'dataset_name_callable', 'dataset_types']:
        assert arg in kwargs, "missing required argument {}".format(arg)
    http_conn_id = kwargs['http_conn_id']
    endpoint = kwargs['endpoint']
    
    ctx = kwargs['dag_run'].conf
    method='POST'
    headers={
        'authorization' : 'Bearer ' + ctx['auth_tok'],
        'content-type' : 'application/json'}
    print('headers:')
    pprint(headers)
    extra_options=[]
    http = HttpHook(method,
                    http_conn_id=http_conn_id)
    data = {
        "source_dataset_uuid": kwargs['parent_dataset_uuid_callable'](**kwargs),
        "derived_dataset_name": kwargs['dataset_name_callable'](**kwargs),
        "derived_dataset_types": kwargs['dataset_types']
    }
    print('data: ')
    pprint(data)
    response = http.run(endpoint,
                        json.dumps(data),
                        headers,
                        extra_options)
    print('response: ')
    pprint(response.json())
    lz_root = split(ctx['parent_lz_path'])[0]
    lz_root = split(lz_root)[0]
    data_dir_path = join(lz_root,
                         response.json()['group_display_name'],
                         response.json()['derived_dataset_uuid'])
    kwargs['ti'].xcom_push(key='group_uuid',
                           value=response.json()['group_uuid'])
    kwargs['ti'].xcom_push(key='derived_dataset_uuid', 
                           value=response.json()['derived_dataset_uuid'])
    return data_dir_path


def pythonop_set_dataset_state(**kwargs) -> None:
    """
    Sets the status of a dataset to 'Processing'
    
    Accepts the following via the caller's op_kwargs:
    'dataset_uuid_callable' : called with **kwargs; returns the
                              uuid of the dataset to be modified
    'http_conn_id' : the http connection to be used
    'endpoint' : the REST endpoint
    'ds_state' : one of 'QA', 'Processing', 'Error', 'Invalid'. Default: 'Processing'
    'message' : update message. Default: 'update state'
    """
    for arg in ['dataset_uuid_callable', 'http_conn_id', 'endpoint']:
        assert arg in kwargs, "missing required argument {}".format(arg)
    dataset_uuid = kwargs['dataset_uuid_callable'](**kwargs)
    http_conn_id = kwargs['http_conn_id']
    endpoint = kwargs['endpoint']
    ds_state = kwargs['ds_state'] if 'ds_state' in kwargs else 'Processing'
    message = kwargs['message'] if 'message' in kwargs else 'update state'
    method='PUT'
    auth_tok = kwargs['auth_tok'] if 'auth_tok' in kwargs else kwargs['dag_run'].conf['auth_tok'] 
    headers={
        'authorization' : 'Bearer ' + auth_tok,
        'content-type' : 'application/json'}
    print('headers:')
    pprint(headers)
    extra_options=[]
     
    http = HttpHook(method,
                    http_conn_id=http_conn_id)

    data = {'dataset_id' : dataset_uuid,
            'status' : ds_state,
            'message' : message,
            'metadata': {}}
    print('data: ')
    pprint(data)

    response = http.run(endpoint,
                        json.dumps(data),
                        headers,
                        extra_options)
    print('response: ')
    pprint(response.json())


def _get_scratch_base_path() -> str:
    scratch_path = airflow_conf.as_dict()['connections']['WORKFLOW_SCRATCH']
    scratch_path = scratch_path.strip("'").strip('"')  # remove quotes that may be on the string
    return scratch_path


def get_tmp_dir_path(run_id: str) -> str:
    """
    Given the run_id, return the path to the dag run's scratch directory
    """
    return join(_get_scratch_base_path(), run_id)


def map_queue_name(raw_queue_name: str) -> str:
    """
    If the configuration contains QUEUE_NAME_TEMPLATE, use it to customize the
    provided queue name.  This allows job separation under Celery.
    """
    if 'QUEUE_NAME_TEMPLATE' in airflow_conf.as_dict()['connections']:
        template = airflow_conf.as_dict()['connections']['QUEUE_NAME_TEMPLATE']
        template = template.strip("'").strip('"')  # remove quotes that may be on the config string
        rslt = template.format(raw_queue_name)
        return rslt
    else:
        return raw_queue_name

def create_dataset_state_error_callback(dataset_uuid_callable: Callable[[Any], str]) -> Callable[[Mapping, Any],
                                                                                                 None]:
    def set_dataset_state_error(contextDict: Mapping, **kwargs) -> None:
        """
        This routine is meant to be 
        """
        msg = 'An internal error occurred in the {} workflow step {}'.format(contextDict['dag'].dag_id,
                                                                             contextDict['task'].task_id)
        new_kwargs = kwargs.copy()
        new_kwargs.update(contextDict)
        new_kwargs.update({'dataset_uuid_callable' : dataset_uuid_callable,
                         'http_conn_id' : 'ingest_api_connection',
                         'endpoint' : '/datasets/status',
                         'ds_state' : 'Error',
                         'message' : msg
                         })
        pythonop_set_dataset_state(**new_kwargs)
    return set_dataset_state_error


set_schema_base_path(SCHEMA_BASE_PATH, SCHEMA_BASE_URI)

def localized_assert_json_matches_schema(jsn: JSONType, schemafile:str) -> None:
    """
    This version of assert_json_matches_schema knows where to find schemata used by this module
    """
    try:
        return assert_json_matches_schema(jsn, schemafile)  # localized by set_schema_base_path
    except AssertionError as e:
        print('ASSERTION FAILED: {}'.format(e))
        raise


def _get_workflow_map() -> List[Tuple[Pattern, Pattern, str]]:
    """
    Lazy compilation of workflow map
    """
    global COMPILED_WORKFLOW_MAP
    if COMPILED_WORKFLOW_MAP is None:
        map_path = join(dirname(__file__), WORKFLOW_MAP_FILENAME)
        with open(map_path, 'r') as f:
            map = yaml.safe_load(f)
        localized_assert_json_matches_schema(map, WORKFLOW_MAP_SCHEMA)
        cmp_map = []
        for dct in map['workflow_map']:
            ct_re = re.compile(dct['collection_type'])
            at_re = re.compile(dct['assay_type'])
            cmp_map.append((ct_re, at_re, dct['workflow']))
        COMPILED_WORKFLOW_MAP = cmp_map
    return COMPILED_WORKFLOW_MAP


def downstream_workflow_iter(collectiontype: str, assay_type: str) -> Iterable[str]:
    """
    Returns an iterator over zero or more workflow names matching the given
    collectiontype and assay_type.  Each workflow name is expected to correspond to
    a known workflow, e.g. an Airflow DAG implemented by workflow_name.py .
    """
    collectiontype = collectiontype or ''
    assay_type = assay_type or ''
    for ct_re, at_re, workflow in _get_workflow_map():
        if ct_re.match(collectiontype) and at_re.match(assay_type):
            yield workflow


def main():
    print(__file__)
    print(get_git_commits([__file__]))
    print(get_git_provenance_dict(__file__))
    dirnm = dirname(__file__)
    if dirnm == '':
        dirnm = '.'
    for elt in get_file_metadata(dirnm):
        print(elt)
    pprint(get_git_provenance_list(__file__))
    md = {'metadata' : {'my_string' : 'hello world'},
          'files' : get_file_metadata(dirnm),
          'dag_provenance_list' : get_git_provenance_list(__file__)}
    try:
        localized_assert_json_matches_schema(md, 'dataset_metadata_schema.yml')
        print('ASSERT passed')
    except AssertionError as e:
        print('ASSERT failed')
    
    assay_pairs = [('devtest', 'devtest'), ('codex', 'CODEX'),
                   ('codex', 'SOMEOTHER'), ('someother', 'CODEX')]
    for collectiontype, assay_type in assay_pairs:
        print('collectiontype {}, assay_type {}:'.format(collectiontype, assay_type))
        for elt in downstream_workflow_iter(collectiontype, assay_type):
            print('  -> {}'.format(elt))
 
 
if __name__ == "__main__":
    main()

