#! /usr/bin/env python

import argparse
import re
import os
import json
from pathlib import Path
from pprint import pprint
from typing import List, Tuple, TypeVar, Union
import logging
from subprocess import run
import requests

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


def run_zip(target_path: Path, zarr_name: str, delete_flg: bool) -> None:
    """
    Zips the directory appropriately for use as a zipped zarr, and deletes
    the original.
    """
    full_target_path = target_path / zarr_name
    assert target_path != target_path.anchor, "zip is aimed at /"
    assert full_target_path != target_path.anchor, "zip is aimed at /"
    zip_name = target_path / f"{zarr_name}.zip"
    cmd0 = "( [[ $PWD != '/' ]] && [[ $PWD != '/hive' ]] )"
    cmd1 = f"cd '{full_target_path}'"
    cmd2 = f"zip -r '{zip_name}' ."
    cmd3 = "cd .."
    cmd4 = f"rm -r -v {zarr_name}"
    if delete_flg:
        full_cmd = f"{cmd0} && {cmd1} && {cmd2} && {cmd3} && {cmd4}"
    else:
        full_cmd = f"{cmd0} && {cmd1} && {cmd2}"
    run(full_cmd, shell=True, check=True)


def restructure(
        candidate: Path,
        dryrun: bool = False,
        delete_flg: bool = False) -> str:
    try:
        target_path, zarr_name, uuid = parse_path(candidate)
    except RuntimeError as excp:
        LOGGER.warning(f"error parsing {candidate}: {excp}")
        return None
    if dryrun:
        LOGGER.info(f"dryrun is true; not zipping {zarr_name}")
    else:
        run_zip(target_path, zarr_name, delete_flg)
    return uuid


def verify_uuid(uuid: str) -> bool:
    """
    Take ENTITY_API and AUTH_TOK from the environment and look up the given uuid.
    Returns True if a valid result is found, False otherwise.
    """
    auth_tok = os.environ.get("AUTH_TOK")
    if not auth_tok:
        raise RuntimeError("AUTH_TOK is not defined in the environment")
    entity_api = os.environ.get("ENTITY_API")
    if not entity_api:
        raise RuntimeError("ENTITY_API is not defined in the environment")
    resp = requests.get(
        url=f"{entity_api}/entities/{uuid}",
        headers={
            "Authorization": f"Bearer {auth_tok}"
        }
    )
    try:
        resp.raise_for_status()
    except Exception as excp:
        LOGGER.error(f"Validation of {uuid} failed: {type(excp)} {excp}")
        return False
    return True


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
        help=("describe the steps that would be taken but"
              " do not make changes"),
        action="store_true",
    )
    parser.add_argument(
        "--delete",
        help="actually rm the non-zipped directory after zipping",
        action="store_true",
    )

    args = parser.parse_args()

    dryrun = args.dryrun
    delete_flg = args.delete

    start_path = Path(args.starting_path)
    if not start_path.is_dir():
        parser.error(f"{start_path} is not an existing directory")

    all_valid_uuids = []
    for candidate in start_path.glob(args.proto):
        LOGGER.info(f"testing {candidate}")
        uuid = restructure(candidate, dryrun=dryrun, delete_flg=delete_flg)
        if uuid and verify_uuid(uuid):
            all_valid_uuids.append(uuid)

    print(f"rebuilding json: {json.dumps({'uuids':all_valid_uuids})}")

if __name__ == "__main__":
    main()
