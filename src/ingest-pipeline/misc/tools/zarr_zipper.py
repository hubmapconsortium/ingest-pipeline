#! /usr/bin/env python

import argparse
import re
from pathlib import Path
from pprint import pprint
from typing import List, Tuple, TypeVar, Union
import logging

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

UUID_REGEX = re.compile(r"[0123456789aAbBcCdDeEfF]{32}")

def parse_path(path) -> Tuple[Path, str, str]:
    LOGGER.debug(f"starting with {path}")
    target_path = path.parent
    uuid_path = target_path
    elts = list(uuid_path.parts)
    LOGGER.debug(f"elts: {elts}")
    elts.reverse()
    for elt in elts:
        LOGGER.debug(f"testing {elt}")
        if UUID_REGEX.match(str(elt)):
            uuid = str(elt)
            break
    else:
        raise RuntimeError("no UUID found")
    return target_path, str(path.name), uuid


def run_zip(target_path: Path, zarr_name: str) -> None:
    full_target_path = target_path / zarr_name
    zip_name = target_path / f"{zarr_name}.zip"
    cmd1 = f"pushd '{full_target_path}'"
    cmd2 = f"echo zip -r '{zip_name}' ."
    cmd3 = "cd .."
    cmd4 = "echo mv command goes here?"
    cmd5 = "popd"
    full_cmd = f"{cmd1} && {cmd2} && {cmd3} && {cmd4} && {cmd5}"
    print(full_cmd)
    """
    cd '/hive/hubmap-dev/data/consortium/IEC Testing Group/672111342c0002f90e1cb1bc5686ecf1/anndata-zarr/reg001_expr-anndata.zarr'
    zip -r ../reg001_expr-anndata.zarr.zip .
    cd ..
    mv reg001_expr-anndata.zarr ../../672111342c0002f90e1cb1bc5686ecf1_anndata-zarr_reg001_expr-anndata.zarr
    """


    


def restructure(candidate: Path, dryrun: bool = False) -> str:
    try:
        target_path, zarr_name, uuid = parse_path(candidate)
    except RuntimeError as excp:
        LOGGER.warn(f"error parsing {candidate}: {excp}")
        return None
    LOGGER.info(f"<{target_path}> <{zarr_name}> <{uuid}>")
    run_zip(target_path, zarr_name)
    return uuid


def main() -> None:
    """
    main
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "starting_path", help="path from which to recur downward looking for zarr dirs"
    )
    parser.add_argument(
        "--proto", help="prototype for zarr search. Remember to quote it!",
        default="**/*anndata.zarr"
    )
    parser.add_argument(
        "--dryrun",
        help="describe the steps that would be taken but" " do not make changes",
        action="store_true",
    )

    args = parser.parse_args()

    dryrun = args.dryrun

    start_path = Path(args.starting_path)
    if not start_path.is_dir():
        parser.error(f"{start_path} is not an existing directory")

    for candidate in start_path.glob(args.proto):
        print(restructure(candidate, dryrun=dryrun))

if __name__ == "__main__":
    main()
