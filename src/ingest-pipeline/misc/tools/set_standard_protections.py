#! /usr/bin/env python

import sys
import os
import argparse
import subprocess
import logging
from pathlib import Path
from io import StringIO
from typing import List

try:
    from survey import Dataset, EntityFactory, is_uuid
except ImportError:
    from .survey import Dataset, EntityFactory, is_uuid


logging.basicConfig()
LOGGER = logging.getLogger(__name__)


def get_uuid_from_cwd() -> str:
    LOGGER.debug("extracting uuid from current working directory %s", os.getcwd())
    for elt in reversed(os.getcwd().split(os.sep)):
        if is_uuid(elt):
            return elt
    raise RuntimeError("no uuid was found in the path to the current" " working directory")


def run_cmd(cmd: List[str]) -> int:
    LOGGER.info("running %s", cmd)
    command = subprocess.run(cmd)
    LOGGER.debug("command returned %s", command.returncode)
    return command.returncode


def process_one_uuid(uuid: str, entity_factory: EntityFactory, **kwargs) -> bool:
    LOGGER.info("handling uuid %s", uuid)
    try:
        ds = entity_factory.get(uuid)
    except AssertionError as e:
        LOGGER.error('Trying to use uuid %s produced the error "%s"', uuid, e)
        return False
    if not isinstance(ds, Dataset):
        LOGGER.error("%s is not a dataset", uuid)
        return False
    buf = StringIO()
    ds.describe(file=buf)
    LOGGER.info("description: %s", buf.getvalue())
    LOGGER.debug("full path: %s", ds.full_path)
    LOGGER.debug("contains_human_genetic_sequences = %s", ds.contains_human_genetic_sequences)

    # acl_fname = "protected_dataset.acl"  # the most restrictive
    if not ds.contains_human_genetic_sequences:
        if ds.status == "Published":
            acl_fname = "public_published.acl"
        else:
            acl_fname = "consortium_dataset.acl"
    else:
        if ds.status == "Published":
            acl_fname = "protected_published_dataset.acl"
        else:
            acl_fname = "protected_dataset.acl"

    acl_path = Path(__file__).absolute().parent.parent.parent / "submodules"
    acl_path = acl_path / "manual-data-ingest" / "acl-settings" / acl_fname
    LOGGER.info("will apply %s", acl_path)

    # Get the dataset full path
    ds_full_path = ds.full_path

    # Set the env param based on the dataset full path
    env = ""
    if "-dev" in str(ds_full_path):
        env = "-dev"
    elif "-test" in str(str(ds_full_path)):
        env = "-test"

    # Get the dataset relative path for the script
    prefix = Path(f"/hive/hubmap{env}/data")
    ds_rel_path = ds_full_path.relative_to(prefix)

    cmd1 = ["sudo", "/usr/local/bin/directory_script.sh", env, str(ds_rel_path), str(acl_path)]
    if kwargs.get("dry_run", False):
        cmd1.insert(1, "--test")
    if run_cmd(cmd1):
        LOGGER.error("Unable to set protections for %s", ds.uuid)
        return False
    return True


def main():
    """
    main
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "uuid",
        nargs="*",
        help=("uuid to set.  May be repeated." "If not present, it is calculated from the CWD"),
    )
    parser.add_argument(
        "--instance",
        required=False,
        help="Infrastructure instance; one of PROD, STAGE, TEST, or DEV.",
        default="PROD",
    )
    parser.add_argument(
        "--dry_run", action="store_true", help="Show acls but do not actually set anything"
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="verbose output (may be repeated for more verbosity)",
    )

    args = parser.parse_args()
    if args.verbose > 1:
        log_level = "DEBUG"
    elif args.verbose == 1:
        log_level = "INFO"
    else:
        log_level = "WARN"
    LOGGER.setLevel(log_level)

    auth_tok = input("auth_tok: ")
    entity_factory = EntityFactory(auth_tok, instance=args.instance)
    uuid_list = args.uuid or [get_uuid_from_cwd()]

    for uuid in uuid_list:
        process_one_uuid(uuid, entity_factory, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
