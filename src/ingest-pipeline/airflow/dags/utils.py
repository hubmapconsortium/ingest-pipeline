from os import environ, walk
from os.path import basename, dirname, relpath, join, getsize
from pathlib import Path
from typing import List
from subprocess import check_call, check_output, CalledProcessError

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
            line = 'notavailable git call failed: {}'.format(e.output)
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
    return {basename(fname) : get_git_commits(fname)
            for fname in file_list}



def _get_file_type(path: str):
    for re
    

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
                         'type': filetype.guess(full_path),
                         'size': getsize(join(root_dir, rp, fn)),
                         'sha1sum': cs})
    return rslt
    
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

