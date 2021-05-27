#! /usr/bin/env python

import sys
import os
import argparse
import subprocess
import logging
from pprint import pprint
from pathlib import Path
from io import StringIO

from survey import (Entity, Dataset, Sample, EntityFactory,
                    ROW_SORT_KEYS, column_sorter, is_uuid,
                    parse_text_list)

logging.basicConfig()
LOGGER = logging.getLogger(__name__)

def get_uuid_from_cwd():
    LOGGER.info('extracting uuid from current working directory %s',
                os.getcwd())
    for elt in reversed(os.getcwd().split(os.sep)):
        if is_uuid(elt):
            return elt
    raise RuntimeError('no uuid was found in the path to the current'
                       ' working directory')


def main():
    """
    main
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("uuid", nargs='?', default=None,
                        help="uuid to set.  If not present, it is calculated from the CWD")
    parser.add_argument("--instance", required=False,
                        help="Infrastructure instance; one of PROD, STAGE, TEST, or DEV.",
                        default="PROD")
    parser.add_argument("--dry_run", action='store_true',
                        help="Show acls but do not actually set anything")
    parser.add_argument("-v", "--verbose", action='store_true',
                        help="verbose output")
    
    args = parser.parse_args()
    LOGGER.setLevel('INFO' if args.verbose else 'WARNING')

    auth_tok = input('auth_tok: ')
    entity_factory = EntityFactory(auth_tok)
    uuid = args.uuid or get_uuid_from_cwd()

    LOGGER.info('uuid is %s', uuid)
    ds = entity_factory.get(uuid)
    if not isinstance(ds, Dataset):
        LOGGER.fatal('%s is not a dataset', uuid)
        sys.exit(f'{uuid} is not a dataset')
    buf = StringIO()
    ds.describe(file=buf)
    LOGGER.info('description: %s', buf.getvalue())
    LOGGER.info('full path: %s', ds.full_path)
    LOGGER.info('contains_human_genetic_sequences = %s',
                ds.contains_human_genetic_sequences)

    acl_fname = 'protected_dataset.acl'  # the most restrictive
    if ds.contains_human_genetic_sequences:
        if ds.status == 'Published':
            acl_fname = 'protected_published_dataset.acl'
        else:
            acl_fname = 'protected_dataset.acl'
    else:
        if ds.status == 'Published':
            acl_fname = 'public_published.acl'
        else:
            acl_fname = 'consortium_dataset.acl'
    acl_path = Path(__file__).absolute().parent.parent.parent / 'submodules'
    acl_path = acl_path / 'manual-data-ingest' / 'acl-settings' / acl_fname
    LOGGER.info('will apply %s', acl_path)
    command = subprocess.run(['setacl', '-b', '-t', ds.full_path])
    LOGGER.info('first command returned %s', command.returncode)
    command = subprocess.run(['setacl', '-R', '-M', '-t', str(acl_path), ds.full_path])
    LOGGER.info('second command returned %s', command.returncode)
    


if __name__ == '__main__':
    main()

