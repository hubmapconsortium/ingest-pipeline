from os import environ
from pathlib import Path
from subprocess import check_call

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
