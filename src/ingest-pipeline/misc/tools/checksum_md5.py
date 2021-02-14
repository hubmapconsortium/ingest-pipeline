#!/usr/bin/env python
# coding: utf-8

import hashlib
import os
import logging
from datetime import date, datetime
from argparse import ArgumentParser
from pathlib import Path
from multiprocessing import Pool, log_to_stderr
from typing import Iterable


LOGGER = log_to_stderr()

DEFAULT_NWORKERS = 10
DEFAULT_OFILE = 'checksum_out.tsv'
FIELDS_TO_KEEP = ['path', 'created', 'scantime', 'created', 'size', 'md5']


def compute_md5sum(file: Path) -> str:
    # BUF_SIZE is totally arbitrary, change for your app!
    BUF_SIZE = 65536  # lets read stuff in 64kb chunks!

    md5 = hashlib.md5()

    if Path(file).is_file():
        with open(file.absolute(), 'rb') as f:
            while True:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                md5.update(data)

    return md5.hexdigest()


def get_file_creation_date(filename: Path) -> str:
    t = os.path.getmtime(str(filename))
    return str(datetime.fromtimestamp(t))


def get_file_size(filename: Path) -> int:
    return filename.stat().st_size


def get_filename(file: Path) -> str:
    return file.name


def build_rec(file: Path) -> dict:
    if file.is_file():
        LOGGER.debug('Reading ' + str(file) + '.')

        record = {'path': str(file),
                  'scantime': date.today().strftime("%d-%m-%Y"),
                  'size': get_file_size(file),
                  'created': get_file_creation_date(file),
                  'md5': compute_md5sum(file)
                  }
    else:
        record = None

    return record


def path_generator(dir_path: Path) -> Iterable[Path]:
    for idx, filepath in enumerate(dir_path.glob('**/*')):
        yield filepath.absolute()


def scan_dir_tree(dir_path: Path, nprocs: int = DEFAULT_NWORKERS,
                  ofile: str = DEFAULT_OFILE) -> None:
    with open(ofile, 'w') as ofile:
        # Write the column header
        ofile.write('\t'.join(FIELDS_TO_KEEP) + '\n')
        with Pool(processes=nprocs) as pool:
            for rec in pool.imap_unordered(build_rec,
                                           path_generator(dir_path)):
                if rec:
                    if '\t' in rec['path']:
                        rec['path'] = '"' + rec['path'] + '"'
                    ofile.write('\t'.join([rec[fld] for fld in FIELDS_TO_KEEP])
                                + '\n')

    LOGGER.info('done')


def main():
    parser = ArgumentParser()
    parser.add_argument('-i', '--input', dest='directory', default=None,
                        help='Data directory')
    parser.add_argument('-d', '--debug', dest='debug', action='store_true',
                        help='Debug')
    parser.add_argument('-n', '--nprocs', dest='nprocs', type=int,
                        help=f'Number of workers (default {DEFAULT_NWORKERS})',
                        default=DEFAULT_NWORKERS)
    parser.add_argument('-o', '--out', dest='ofile', type=str,
                        help=f'Name for output .tsv file '
                        '(default {DEFAULT_OFILE})',
                        default=DEFAULT_OFILE)
    results = parser.parse_args()
    if results.debug:
        LOGGER.setLevel(logging.DEBUG)
    else:
        LOGGER.setLevel(logging.INFO)
    scan_dir_tree(Path(results.directory), nprocs=results.nprocs,
                  ofile=results.ofile)


if __name__ == "__main__":
    main()
