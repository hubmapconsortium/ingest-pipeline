from os import environ, walk
from os.path import basename, dirname, relpath, split, join, getsize, realpath
from pathlib import Path
from typing import List
from subprocess import check_call, check_output, CalledProcessError
import re
import json
from pprint import pprint

from airflow.hooks.http_hook import HttpHook

# Some constants
PIPELINE_BASE_DIR = Path(environ['AIRFLOW_HOME']) / 'pipeline_git_repos'

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
    '--oneline',
    '{fname}',
]
SHA1SUM_COMMAND = [
    'sha1sum',
    '{fname}'
]
FILE_TYPE_MATCHERS = [('^.*\.csv$', 'csv'),  # format is (regex, type)
                      ('^.*\.hdf5$', 'hdf5'),
                      ('^.*\.h5ad$', 'h5ad'),
                      ('^.*\.pdf$', 'pdf'),
                      ('^.*\.json$', 'json'),
                      ('^.*\.arrow$', 'arrow'),
                      ('(^.*\.fastq$)|(^.*\.fastq.gz$)', 'fastq'),
                      ('(^.*\.yml$)|(^.*\.yaml$)', 'yaml')
                      ]
COMPILED_TYPE_MATCHERS = None


def clone_or_update_pipeline(pipeline_name: str, ref: str = 'origin/master'):
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


def get_git_commits(file_list: List[str] or str):
    rslt = []
    if not isinstance(file_list, list):
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


def get_git_provenance_dict(file_list: List[str] or str):
    if not isinstance(file_list, list):
        file_list = [file_list]
    return {basename(fname) : get_git_commits(realpath(fname))
            for fname in file_list}



def _get_file_type(path: str):
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
    

def get_file_metadata(root_dir: str):
    rslt = []
    for dirpth, dirnames, fnames in walk(root_dir):
        rp = relpath(dirpth, start=root_dir)
        for fn in fnames:
            full_path = join(root_dir, rp, fn)
            sz = getsize(full_path)
            line = check_output([word.format(fname=full_path)
                                 for word in SHA1SUM_COMMAND])
            cs = line.split()[0].strip().decode('utf-8')
            rslt.append({'rel_path': join(rp, fn),
                         'type': _get_file_type(full_path),
                         'size': getsize(join(root_dir, rp, fn)),
                         'sha1sum': cs})
    return rslt


def pythonop_trigger_target(**kwargs):
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
 

def pythonop_maybe_keep(**kwargs):
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


def pythonop_send_create_dataset(**kwargs):
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


def pythonop_set_dataset_state(**kwargs):
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
    headers={
        'authorization' : 'Bearer ' + kwargs['dag_run'].conf['auth_tok'],
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


def get_tmp_dir_path(run_id):
    """
    Given the run_id, return the path to the dag run's scratch directory
    """
    return "{}/data/temp/{}".format(environ['AIRFLOW_HOME'], run_id)

def main():
    print(__file__)
    print(get_git_commits([__file__]))
    print(get_git_provenance_dict(__file__))
    dirnm = dirname(__file__)
    if dirnm == '':
        dirnm = '.'
    for elt in get_file_metadata(dirnm):
        print(elt)
 
 
if __name__ == "__main__":
    main()

