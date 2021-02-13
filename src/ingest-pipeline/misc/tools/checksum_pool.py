#!/usr/bin/env python
# coding: utf-8

import hashlib
import time
import os
import datetime
import subprocess
import uuid
import socket
from datetime import date
from argparse import ArgumentParser
from pathlib import Path
from multiprocessing import Pool, log_to_stderr
from typing import Iterable
import logging


LOGGER = log_to_stderr()

DEFAULT_NWORKERS = 10
DEFAULT_OFILE = 'checksum_out.tsv'
FIELDS_TO_KEEP = ['path', 'created', 'scantime', 'uuid', 'sha256']


def compute_xxh64sum(filename: Path) -> str:
    if Path(filename).is_file():
        results = subprocess.check_output('xxh64sum '
                                          + str(filename)
                                          + ' | cut -d" " -f1 | xargs',
                                          shell=True)
        return results.decode("utf-8").strip()


def compute_sha256sum(file: Path) -> str:
    # BUF_SIZE is totally arbitrary, change for your app!
    BUF_SIZE = 65536  # lets read stuff in 64kb chunks!

    sha256 = hashlib.sha256()

    if Path(file).is_file():
        with open(file.absolute(), 'rb') as f:
            while True:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                sha256.update(data)

    return sha256.hexdigest()


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


def compute_sha1sum(filename: Path) -> str:
    # BUF_SIZE is totally arbitrary, change for your app!
    BUF_SIZE = 65536  # lets read stuff in 64kb chunks!

    sha1 = hashlib.sha1()

    if Path(filename).is_file():
        with open(filename.absolute(), 'rb') as f:
            while True:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                sha1.update(data)

    return sha1.hexdigest()


#
# def compute_xxh128sum(filename: Path) -> str:
#     if Path(filename).is_file():
#         command = 'xxh128sum "' + str(filename) + '" | cut -d" " -f1 | xargs'
#         results = subprocess.check_output(command, shell=True)
#         return results.decode("utf-8").strip()
#


def generate_uuid() -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, socket.getfqdn()))


def get_file_creation_date(filename: Path) -> str:
    t = os.path.getmtime(str(filename))
    return str(datetime.datetime.fromtimestamp(t))


def get_file_size(filename: Path) -> int:
    return filename.stat().st_size


def get_filename(file: Path) -> str:
    return file.name


def build_rec(file: Path) -> dict:
    if file.is_file():
        LOGGER.debug('Reading ' + str(file) + '.')

        # compute md5sum
        start_time = time.time()
        md5sum = compute_md5sum(file)
        md5sum_running_time = time.time() - start_time

        # compute sha256sum
        start_time = time.time()
        sha256sum = compute_sha256sum(file)
        sha256sum_running_time = time.time() - start_time

        # #compute xxh128sum
        # start_time = time.time()
        # xxh128sum = compute_xxh128sum(file)
        # xxh128sum_running_time = time.time() - start_time

        record = {'path': str(file),
                  'uuid': generate_uuid(),
                  'scantime': date.today().strftime("%d-%m-%Y"),
                  'size': get_file_size(file),
                  'created': get_file_creation_date(file),
                  'md5': md5sum,
                  'md5sum_time': md5sum_running_time,
                  'sha256': sha256sum,
                  'sha256_time': sha256sum_running_time,
                  # 'xxh128': xxh128sum,
                  # 'xxh128_time': xxh128sum_running_time
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
