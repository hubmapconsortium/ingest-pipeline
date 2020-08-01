from abc import ABC, abstractmethod
from functools import lru_cache
import json
from os import environ, fspath, walk
from os.path import basename, dirname, relpath, split, join, getsize, realpath
from pathlib import Path
from pprint import pprint
import re
import shlex
from subprocess import check_output, CalledProcessError
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Pattern, Tuple, TypeVar, Union
from requests.exceptions import HTTPError
from requests import codes
import uuid

from airflow.configuration import conf as airflow_conf
from airflow.hooks.http_hook import HttpHook
from cryptography.fernet import Fernet
import yaml

from hubmap_commons.schema_tools import assert_json_matches_schema, set_schema_base_path

import cwltool  # used to find its path


JSONType = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]

# Some functions accept a `str` or `List[str]` and return that same type
StrOrListStr = TypeVar('StrOrListStr', str, List[str])

PathStrOrList = Union[str, Path, Iterable[Union[str, Path]]]

SCHEMA_BASE_PATH = join(dirname(dirname(dirname(realpath(__file__)))),
                        'schemata')
SCHEMA_BASE_URI = 'http://schemata.hubmapconsortium.org/'

# Some constants
PIPELINE_BASE_DIR = Path(__file__).resolve().parent / 'cwl'

RE_ID_WITH_SLICES = re.compile(r'([a-zA-Z0-9\-]*)-(\d*)_(\d*)')

RE_GIT_URL_PATTERN = re.compile('(^git@github.com:)(.*)(\.git)')

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


class FileMatcher(ABC):
    @abstractmethod
    def get_file_metadata(self, file_path: Path) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        :return: A 3-tuple:
         [0] bool, whether to add `file_path` to a downstream index
         [1] formatted description if [0] is True, otherwise None
         [2] EDAM ontology term if [0] is True, otherwise None
        """


class PipelineFileMatcher(FileMatcher):
    # (file/directory regex, description template, EDAM ontology term)
    matchers: List[Tuple[Pattern, str, str]]

    def __init__(self):
        self.matchers = []

    @classmethod
    def read_manifest(cls, pipeline_file_manifest: Path) -> Iterable[Tuple[Pattern, str, str]]:
        with open(pipeline_file_manifest) as f:
            manifest = json.load(f)
            localized_assert_json_matches_schema(manifest, 'pipeline_file_manifest.yml')

        for annotation in manifest:
            pattern = re.compile(annotation['pattern'])
            yield pattern, annotation['description'], annotation['edam_ontology_term']

    @classmethod
    def create_from_files(cls, pipeline_file_manifests: Iterable[Path]):
        obj = cls()
        for manifest in pipeline_file_manifests:
            obj.matchers.extend(cls.read_manifest(manifest))
        return obj

    def get_file_metadata(self, file_path: Path) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Checks `file_path` against the list of patterns stored in this object.
        At the first match, return the associated description and ontology term.
        If no match, return `None`. Patterns are ordered in the JSON file, so
        the "first-match" behavior is deliberate.
        """
        path_str = fspath(file_path)
        for pattern, description_template, ontology_term in self.matchers:
            # TODO: walrus operator
            m = pattern.search(path_str)
            if m:
                formatted_description = description_template.format_map(m.groupdict())
                return True, formatted_description, ontology_term
        return False, None, None


class DummyFileMatcher(FileMatcher):
    """
    Drop-in replacement for PipelineFileMatcher which allows everything and always
    provides empty descriptions and ontology terms.
    """
    def get_file_metadata(self, file_path: Path) -> Tuple[bool, Optional[str], Optional[str]]:
        return True, '', ''


def find_pipeline_manifests(cwl_files: Iterable[Path]) -> List[Path]:
    """
    Constructs manifest paths from CWL files (strip '.cwl', append
    '-manifest.json'), and check whether each manifest exists. Return
    a list of `Path`s that exist on disk.
    """
    manifests = []
    for cwl_file in cwl_files:
        manifest_file = cwl_file.with_name(f'{cwl_file.stem}-manifest.json')
        if manifest_file.is_file():
            manifests.append(manifest_file)
    return manifests


def get_absolute_workflows(*workflows: Path) -> List[Path]:
    """
    :param workflows: iterable of `Path`s to CWL files, absolute
      or relative
    :return: Absolute paths to workflows: if the input paths were
      already absolute, they are returned unchanged; if relative,
      they are anchored to `PIPELINE_BASE_DIR`
    """
    return [
        PIPELINE_BASE_DIR / workflow
        for workflow in workflows
    ]


def get_parent_dataset_uuid(**kwargs):
    return kwargs['dag_run'].conf['parent_submission_id']


def get_dataset_uuid(**kwargs):
    return kwargs['ti'].xcom_pull(key='derived_dataset_uuid',
                                  task_ids="send_create_dataset")


def get_uuid_for_error(**kwargs):
    """
    Return the uuid for the derived dataset if it exists, and of the parent dataset otherwise.
    """
    rslt = get_dataset_uuid(**kwargs)
    if rslt is None:
        rslt = get_parent_dataset_uuid(**kwargs)
    return rslt


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


def _convert_git_to_proper_url(raw_url: str) -> str:
    """
    If the provided string is of the form git@github.com:something.git, return
    https://github.com/something .  Otherwise just return the input string.
    """
    m = RE_GIT_URL_PATTERN.fullmatch(raw_url)
    if m:
        return f'https://github.com/{m[2]}'
    else:
        return raw_url
    

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
        url = _convert_git_to_proper_url(url)
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


def get_git_provenance_dict(file_list: PathStrOrList) -> Mapping[str, str]:
    """
    Given a list of file paths, return a list of dicts of the form:
    
      [{<file base name>:<file commit hash>}, ...]
    """
    if isinstance(file_list, (str, Path)):  # sadly, a str is an Iterable[str]
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


def _get_file_type(path: Path) -> str:
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
        if regex.match(fspath(path)):
            return tpnm
    return 'unknown'


def get_file_metadata(root_dir: str, matcher: FileMatcher) -> List[Mapping[str, Any]]:
    """
    Given a root directory, return a list of the form:
    
      [
        {
          'rel_path': <relative path>,
          'type': <file type>,
          'size': <file size>,
          'description': <human-readable file description>,
          'edam_term': <EDAM ontology term>,
        },
        ...
      ]
    
    containing an entry for every file below the given root directory:
    """
    root_path = Path(root_dir)
    rslt = []
    for dirpth, dirnames, fnames in walk(root_dir):
        dp = Path(dirpth)
        for fn in fnames:
            full_path = dp / fn
            relative_path = full_path.relative_to(root_path)
            add_to_index, description, ontology_term = matcher.get_file_metadata(relative_path)
            if add_to_index:
                # sha1sum disabled because of run time issues on large data collections
                #line = check_output([word.format(fname=full_path)
                #                     for word in SHA1SUM_COMMAND])
                #cs = line.split()[0].strip().decode('utf-8')
                rslt.append(
                    {
                        'rel_path': fspath(relative_path),
                        'type': _get_file_type(full_path),
                        'size': getsize(full_path),
                        'description': description,
                        'edam_term': ontology_term,
                         #'sha1sum': cs,
                    }
                )
    return rslt

def get_file_metadata_dict(
        root_dir: str,
        alt_file_dir: str,
        pipeline_file_manifests: List[Path],
        max_in_line_files: int = MAX_IN_LINE_FILES,
) -> Mapping[str, Any]:
    """
    This routine returns file metadata, either directly as JSON in the form
    {'files': [{...}, {...}, ...]} with the list returned by get_file_metadata() or the form
    {'files_info_alt_path': path} where path is the path of a unique file in alt_file_dir
    relative to the WORKFLOW_SCRATCH config parameter
    """
    if not pipeline_file_manifests:
        matcher = DummyFileMatcher()
    else:
        matcher = PipelineFileMatcher.create_from_files(pipeline_file_manifests)
    file_info = get_file_metadata(root_dir, matcher)
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
 

def pythonop_maybe_keep(**kwargs) -> str:
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
    if retcode == 0:
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
    either                             
      'dataset_types' : the types list of the new dataset
    or
      'dataset_types_callable' : called with **kwargs; returns the
                                 types list of the new dataset
    
    Returns the following via XCOM:
    (no key) : data_directory_path for the new dataset
    'derived_dataset_uuid' : uuid for the created dataset
    'group_uuid' : group uuid for the created dataset
    """
    for arg in ['parent_dataset_uuid_callable', 'http_conn_id', 'endpoint',
                'dataset_name_callable', 'dataset_types']:
        assert arg in kwargs, "missing required argument {}".format(arg)
    for arg_options in [['dataset_types', 'dataset_types_callable']]:
        assert any([arg in kwargs for arg in arg_options])
    http_conn_id = kwargs['http_conn_id']
    endpoint = kwargs['endpoint']
    
    ctx = kwargs['dag_run'].conf
    method='POST'
    crypt_auth_tok = (kwargs['crypt_auth_tok'] if 'crypt_auth_tok' in kwargs
                      else kwargs['dag_run'].conf['crypt_auth_tok'])
    auth_tok = ''.join(e for e in decrypt_tok(crypt_auth_tok.encode())
                       if e.isalnum())  # strip out non-alnum characters
    headers={
        'authorization' : 'Bearer ' + auth_tok,
        'content-type' : 'application/json'}
    #print('headers:')
    #pprint(headers)  # Reduce exposure of auth_tok
    extra_options=[]
    http = HttpHook(method,
                    http_conn_id=http_conn_id)
    if 'dataset_types' in kwargs:
        dataset_types = kwargs['dataset_types']
    else:
        dataset_types = kwargs['dataset_types_callable'](**kwargs)
    dataset_name = kwargs['dataset_name_callable'](**kwargs)
    data = {
        "source_dataset_uuid": kwargs['parent_dataset_uuid_callable'](**kwargs),
        "derived_dataset_name": dataset_name,
        "derived_dataset_types": dataset_types
    }
    print('data:')
    pprint(data)
    response = http.run(endpoint,
                        json.dumps(data),
                        headers,
                        extra_options)
    print('response: ')
    pprint(response.json())
    data_dir_path = response.json()['full_path']
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
    crypt_auth_tok = (kwargs['crypt_auth_tok'] if 'crypt_auth_tok' in kwargs
                      else kwargs['dag_run'].conf['crypt_auth_tok'])
    headers={
        'authorization' : 'Bearer ' + decrypt_tok(crypt_auth_tok.encode()),
        'content-type' : 'application/json'}
#     print('headers:')
#     pprint(headers)  # reduce visibility of auth_tok
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


def pythonop_get_dataset_state(**kwargs) -> JSONType:
    """
    Gets the status JSON structure for a dataset.
    
    Accepts the following via the caller's op_kwargs:
    'dataset_uuid_callable' : called with **kwargs; returns the
                              uuid of the dataset to be modified
    'http_conn_id' : the http connection to be used
    """
    for arg in ['dataset_uuid_callable', 'http_conn_id']:
        assert arg in kwargs, "missing required argument {}".format(arg)
    dataset_uuid = kwargs['dataset_uuid_callable'](**kwargs)
    http_conn_id = kwargs['http_conn_id']
    endpoint = f'datasets/{dataset_uuid}'
    method='GET'
    crypt_auth_tok = (kwargs['crypt_auth_tok'] if 'crypt_auth_tok' in kwargs
                      else kwargs['dag_run'].conf['crypt_auth_tok'])
    auth_tok = ''.join(e for e in decrypt_tok(crypt_auth_tok.encode())
                       if e.isalnum())  # strip out non-alnum characters
    headers={
        'authorization' : f'Bearer {auth_tok}',
        'content-type' : 'application/json'}

    try:
        http = HttpHook(method,
                        http_conn_id=http_conn_id)

        response = http.run(endpoint,
                            headers=headers,
                            extra_options={'check_response': False})
        response.raise_for_status()
    except HTTPError as e:
        print(f'ERROR: {e}')
        if e.response.status_code == codes.unauthorized:
            raise RuntimeError('entity database authorization was rejected?')
        else:
            print('benign error')
            return {}
    return response.json()


def _uuid_lookup(uuid, **kwargs):
    http_conn_id = 'uuid_api_connection'
    endpoint = 'hmuuid/{}'.format(uuid)
    method='GET'
    crypt_auth_tok = (kwargs['crypt_auth_tok'] if 'crypt_auth_tok' in kwargs
                      else kwargs['dag_run'].conf['crypt_auth_tok'])
    headers={'authorization' : 'Bearer ' + decrypt_tok(crypt_auth_tok.encode())}
#     print('headers:')
#     pprint(headers)
    extra_options=[]
     
    http = HttpHook(method,
                    http_conn_id=http_conn_id)

    response = http.run(endpoint,
                        None,
                        headers,
                        extra_options)
#     print('response: ')
#     pprint(response.json())
    return response.json()


def _generate_slices(id: str) -> Iterable[str]:
    mo = RE_ID_WITH_SLICES.fullmatch(id)
    if mo:
        base, lidx, hidx = mo.groups()
        lidx = int(lidx)
        hidx = int(hidx)
        for idx in range(lidx, hidx+1):
            yield(f'{base}-{idx}')
    else:
        yield id


def assert_id_known(id: str, **kwargs) -> None:
    """
    Is the given id string known to the uuid database?  Id strings with suffixes like
    myidstr-n1_n2 where n1 and n2 are integers are interpreted as representing multiple
    ids with suffix integers in the range n1 to n2 inclusive.
    
    Raises AssertionError if the ID is not known.
    """
    for slice in _generate_slices(id):
        tissue_info = _uuid_lookup(slice, **kwargs)
        assert tissue_info and len(tissue_info) >= 1, f'tissue_id {slice} not found on lookup'


def pythonop_md_consistency_tests(**kwargs) -> int:
    """
    Perform simple consistency checks of the metadata stored as YAML in kwargs['metadata_fname'].
    This includes accessing the UUID api via its Airflow connection ID to verify uuids.
    """
    md_path = join(get_tmp_dir_path(kwargs['run_id']), kwargs['metadata_fname'])
    with open(md_path, 'r') as f:
        md = yaml.safe_load(f)
#     print('metadata from {} follows:'.format(md_path))
#     pprint(md)
    if '_from_metadatatsv' in md and md['_from_metadatatsv']:
        try:
            for elt in ['tissue_id', 'donor_id']:
                assert elt in md, 'metadata is missing {}'.format(elt)
            assert md['tissue_id'].startswith(md['donor_id']+'-'), 'tissue_id does not match'
            assert_id_known(md['tissue_id'], **kwargs)
            return 0
        except AssertionError as e:
            kwargs['ti'].xcom_push(key='err_msg',
                                   value='Assertion Failed: {}'.format(e))
            return 1
    else:
        return 0

def _get_scratch_base_path() -> str:
    dct = airflow_conf.as_dict(display_sensitive=True)['connections']
    if 'WORKFLOW_SCRATCH' in dct:
        scratch_path = dct['WORKFLOW_SCRATCH']
    elif 'workflow_scratch' in dct:
        # support for lower case is necessary setting the scratch path via the
        # environment variable AIRFLOW__CONNECTIONS__WORKFLOW_SCRATCH
        scratch_path = dct['workflow_scratch']
    else:
        raise KeyError('WORKFLOW_SCRATCH')  # preserve original code behavior
    scratch_path = scratch_path.strip("'").strip('"')  # remove quotes that may be on the string
    return scratch_path


def get_tmp_dir_path(run_id: str) -> str:
    """
    Given the run_id, return the path to the dag run's scratch directory
    """
    return join(_get_scratch_base_path(), run_id)


@lru_cache(maxsize=1)
def get_cwltool_bin_path() -> Path:
    """
    Returns the full path to the cwltool binary
    """
    cwltool_dir = dirname(cwltool.__file__)
    while cwltool_dir:
        part1, part2 = split(cwltool_dir)
        cwltool_dir = part1
        if part2 == 'lib':
            break
    assert cwltool_dir, 'Failed to find cwltool bin directory'
    cwltool_dir = Path(cwltool_dir, 'bin')
    return cwltool_dir


def get_cwltool_base_cmd(tmpdir: Path) -> List[str]:
    return [
        'env',
        'PATH={}:{}'.format(get_cwltool_bin_path(), environ['PATH']),
        'TMPDIR={}'.format(tmpdir),
        'cwltool',
        # The trailing slashes in the next two lines are deliberate.
        # cwltool treats these path prefixes as *strings*, not as
        # directories in which new temporary dirs should be created, so
        # a path prefix of '/tmp/cwl-tmp' will cause cwltool to use paths
        # like '/tmp/cwl-tmpXXXXXXXX' with 'XXXXXXXX' as a random string.
        # Adding the trailing slash is ensures that temporary directories
        # are created as *subdirectories* of 'cwl-tmp' and 'cwl-out-tmp'.
        '--tmpdir-prefix={}/'.format(tmpdir / 'cwl-tmp'),
        '--tmp-outdir-prefix={}/'.format(tmpdir / 'cwl-out-tmp'),
    ]

def make_send_status_msg_function(
        dag_file: str,
        retcode_ops: List[str],
        cwl_workflows: List[Path],
):
    """
    `dag_file` should always be `__file__` wherever this function is used,
    to include the DAG file in the provenance. This could be "automated" with
    something like `sys._getframe(1).f_code.co_filename`, but that doesn't
    seem worth it at the moment
    """
    def send_status_msg(**kwargs):
        retcodes = [
            int(kwargs['ti'].xcom_pull(task_ids=op))
            for op in retcode_ops
        ]
        print('retcodes: ', {k: v for k, v in zip(retcode_ops, retcodes)})
        success = all(rc == 0 for rc in retcodes)
        derived_dataset_uuid = kwargs['ti'].xcom_pull(
            key='derived_dataset_uuid',
            task_ids='send_create_dataset',
        )
        ds_dir = kwargs['ti'].xcom_pull(task_ids='send_create_dataset')
        http_conn_id = 'ingest_api_connection'
        endpoint = '/datasets/status'
        method = 'PUT'
        crypt_auth_tok = kwargs['dag_run'].conf['crypt_auth_tok']
        headers = {
            'authorization': 'Bearer ' + decrypt_tok(crypt_auth_tok.encode()),
            'content-type': 'application/json',
        }
        extra_options = []

        http = HttpHook(method, http_conn_id=http_conn_id)

        if success:
            md = {}
            files_for_provenance = [dag_file, *cwl_workflows]

            if 'dag_provenance' in kwargs['dag_run'].conf:
                md['dag_provenance'] = kwargs['dag_run'].conf['dag_provenance'].copy()
                new_prv_dct = get_git_provenance_dict(files_for_provenance)
                md['dag_provenance'].update(new_prv_dct)
            else:
                dag_prv = (kwargs['dag_run'].conf['dag_provenance_list']
                           if 'dag_provenance_list' in kwargs['dag_run'].conf
                           else [])
                dag_prv.extend(get_git_provenance_list(files_for_provenance))
                md['dag_provenance_list'] = dag_prv

            manifest_files = find_pipeline_manifests(cwl_workflows)
            md.update(
                get_file_metadata_dict(
                    ds_dir,
                    get_tmp_dir_path(kwargs['run_id']),
                    manifest_files,
                ),
            )
            try:
                assert_json_matches_schema(md, 'dataset_metadata_schema.yml')
                data = {
                    'dataset_id': derived_dataset_uuid,
                    'status': 'QA',
                    'message': 'the process ran',
                    'metadata': md,
                }
            except AssertionError as e:
                print('invalid metadata follows:')
                pprint(md)
                data = {
                    'dataset_id': derived_dataset_uuid,
                    'status': 'Error',
                    'message': 'internal error; schema violation: {}'.format(e),
                    'metadata': {},
                }
        else:
            log_fname = Path(get_tmp_dir_path(kwargs['run_id']), 'session.log')
            with open(log_fname, 'r') as f:
                err_txt = '\n'.join(f.readlines())
            data = {
                'dataset_id': derived_dataset_uuid,
                'status': 'Invalid',
                'message': err_txt,
            }
        print('data: ')
        pprint(data)

        response = http.run(
            endpoint,
            json.dumps(data),
            headers,
            extra_options,
        )
        print('response: ')
        pprint(response.json())

    return send_status_msg


def map_queue_name(raw_queue_name: str) -> str:
    """
    If the configuration contains QUEUE_NAME_TEMPLATE, use it to customize the
    provided queue name.  This allows job separation under Celery.
    """
    conf_dict = airflow_conf.as_dict()
    if 'QUEUE_NAME_TEMPLATE' in conf_dict.get('connections', {}):
        template = conf_dict['connections']['QUEUE_NAME_TEMPLATE']
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


def downstream_workflow_iter(collectiontype: str, assay_type: StrOrListStr) -> Iterable[str]:
    """
    Returns an iterator over zero or more workflow names matching the given
    collectiontype and assay_type.  Each workflow name is expected to correspond to
    a known workflow, e.g. an Airflow DAG implemented by workflow_name.py .
    """
    collectiontype = collectiontype or ''
    assay_type = assay_type or ''
    for ct_re, at_re, workflow in _get_workflow_map():
        if isinstance(assay_type, str):
            at_match = at_re.match(assay_type)
        else:
            at_match = all(at_re.match(elt) for elt in assay_type)
        if ct_re.match(collectiontype) and at_match:
            yield workflow


def encrypt_tok(cleartext_tok: str) -> bytes:
    key = airflow_conf.as_dict(display_sensitive=True)['core']['fernet_key']
    fernet = Fernet(key.encode())
    return fernet.encrypt(cleartext_tok.encode())


def decrypt_tok(crypt_tok: bytes) -> str:
    key = airflow_conf.as_dict(display_sensitive=True)['core']['fernet_key']
    fernet = Fernet(key.encode())
    return fernet.decrypt(crypt_tok).decode()


def join_quote_command_str(pieces: List[Any]):
    command_str = ' '.join(shlex.quote(str(piece)) for piece in pieces)
    print('final command_str:', command_str)
    return command_str

def main():
    print(__file__)
    print(get_git_commits([__file__]))
    print(get_git_provenance_dict(__file__))
    dirnm = dirname(__file__)
    if dirnm == '':
        dirnm = '.'
    for elt in get_file_metadata(dirnm, DummyFileMatcher()):
        print(elt)
    pprint(get_git_provenance_list(__file__))
    md = {'metadata' : {'my_string' : 'hello world'},
          'files' : get_file_metadata(dirnm, DummyFileMatcher()),
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
    
    print(f'cwltool bin path: {get_cwltool_bin_path()}')

    s = 'hello world'
    crypt_s = encrypt_tok(s)
    s2 = decrypt_tok(crypt_s)
    print('crypto test: {} -> {} -> {}'.format(s, crypt_s, s2))
 
 
if __name__ == "__main__":
    main()

