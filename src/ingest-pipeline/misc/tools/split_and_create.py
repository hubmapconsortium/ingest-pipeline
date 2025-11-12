#! /usr/bin/env python

import argparse
import json
import re
import time
from pathlib import Path
from pprint import pprint
from shutil import copy2, copytree
from typing import List, Tuple, TypeVar, Union

import pandas as pd
import numpy as np
from extra_utils import SoftAssayClient
from status_change.status_manager import StatusChanger, Statuses

from airflow.providers.http.hooks.http import HttpHook

# There has got to be a better solution for this, but I can't find it
try:
    from survey import ENDPOINTS, Dataset, EntityFactory, Upload
except ImportError:
    from .survey import ENDPOINTS, Dataset, EntityFactory, Upload


DEFAULT_FROZEN_DF_FNAME = "frozen_source_df{}.tsv"  # must work with frozen_name.format(suffix)
FAKE_UUID_GENERATOR = None
SCRATCH_PATH = "/tmp/split_and_create"

StrOrListStr = TypeVar("StrOrListStr", str, List[str])

#
# The following are used to try to deal with bad assay type information in the original
# upload or the metadata.tsv file, and with new assay types which have not yet been
# deployed to PROD.
#
FALLBACK_ASSAY_TYPE_TRANSLATIONS = {
    # 'SNARE-Seq2-AC': 'SNARE-ATACseq2',
    "SNARE-Seq2-AC": "SNAREseq",
    # 'SNARE2-RNAseq': 'SNARE-RNAseq2',
    "SNARE2-RNAseq": "sciRNAseq",
    "scRNAseq-10xGenomics-v2": "scRNA-Seq-10x",
}


#
# Some cases need specialized transformations.  For example, when an input upload
# is validated with an earlier version of the validation tests, but will fail if
# re-validated with the current version.  These transformations can be used to
# bring the metadata into compliance with the current validation rules on the fly.
#
def _remove_na(row: pd.Series, parent_assay_type: StrOrListStr) -> pd.Series:
    new_row = row.copy()
    key = "transposition_kit_number"
    if key in row and row[key].lower() == "na":
        new_row[key] = ""
    return new_row


SEQ_RD_FMT_TEST_RX = re.compile(r"\d+\+\d+\+\d+\+\d+")


def _reformat_seq_read(row: pd.Series, parent_assay_type: StrOrListStr) -> pd.Series:
    new_row = row.copy()
    key = "sequencing_read_format"
    if key in row and SEQ_RD_FMT_TEST_RX.match(row[key]):
        new_row[key] = row[key].replace("+", "/")
    return new_row


def _fix_snare_atac_assay_type(row: pd.Series, parent_assay_type: StrOrListStr) -> pd.Series:
    new_row = row.copy()
    key1 = "assay_type"
    key2 = "canonical_assay_type"
    if key1 in row and key2 in row and row[key1] == "SNARE-seq2" and row[key2] == "SNAREseq":
        new_row[key2] = "SNARE-seq2"
    return new_row


SPECIAL_CASE_TRANSFORMATIONS = [
    (re.compile("SNAREseq"), [_remove_na, _reformat_seq_read, _fix_snare_atac_assay_type])
]


def create_fake_uuid_generator():
    """This is used to simulate unique uuids for dryrun executions"""
    count = 0
    while True:
        rslt = "fakeuuid_%08x" % count
        count += 1
        yield rslt


def get_canonical_assay_type(row, dataset_type=None):
    # TODO: check if this needs to be rewrite to support old style metadata
    return (
        dataset_type
        if dataset_type is not None
        else (row["assay_type"] if hasattr(row, "assay_type") else row["dataset_type"])
    )


def create_new_uuid(row, source_entity, entity_factory, primary_entity, dryrun=False):
    """
    Use the entity_factory to create a new dataset, with safety checks
    """
    global FAKE_UUID_GENERATOR
    canonical_assay_type = row["canonical_assay_type"]
    # orig_assay_type = row["assay_type"] if hasattr(row, "assay_type") else row["dataset_type"]
    info_txt = None
    if isinstance(source_entity, Dataset):
        assert "lab_dataset_id" in source_entity.prop_dct, (
            f"Dataset {source_entity.uuid}" " has no lab_dataset_id"
        )
        info_txt = ""
    elif isinstance(source_entity, Upload):
        if "title" in source_entity.prop_dct:
            info_txt = source_entity.prop_dct["title"]
        else:
            print(f"WARNING: Upload {source_entity.uuid} has no title")
            info_txt = f"Upload {source_entity.prop_dct['hubmap_id']}"
    assert info_txt is not None, "Expected a Dataset or an Upload"
    contains_human_genetic_sequences = primary_entity.get("contains-pii")
    is_epic = primary_entity.get("is-epic")
    # Check consistency in case this is a Dataset, which will have this info
    if "contains_human_genetic_sequences" in source_entity.prop_dct:
        assert (
            contains_human_genetic_sequences
            == source_entity.prop_dct["contains_human_genetic_sequences"]
        )

    priority_project_list = source_entity.prop_dct.get("priority_project_list", [])

    group_uuid = source_entity.prop_dct["group_uuid"]
    if "description" in row:
        description = str(row["description"])
    elif "description" in source_entity.prop_dct:
        description = source_entity.prop_dct["description"]
    else:
        description = ""
    sample_id_list = (
        (row["tissue_id"] if hasattr(row, "tissue_id") else row["parent_sample_id"])
        if not is_epic
        else row.get("parent_dataset_id")
    )
    direct_ancestor_uuids = []
    for sample_id in sample_id_list.split(","):
        sample_id = sample_id.strip()
        sample_uuid = entity_factory.id_to_uuid(sample_id)
        print(f"including tissue_id/dataset_id {sample_id} ({sample_uuid})")
        direct_ancestor_uuids.append(sample_uuid)
    direct_ancestor_uuids = list(set(direct_ancestor_uuids))  # remove any duplicates
    assert direct_ancestor_uuids, "No tissue ids found?"

    if dryrun:
        if FAKE_UUID_GENERATOR is None:
            FAKE_UUID_GENERATOR = create_fake_uuid_generator()
        uuid = FAKE_UUID_GENERATOR.__next__()
        print(f"Not creating uuid {uuid} with assay_type {canonical_assay_type}")
        return uuid
    else:
        rslt = entity_factory.create_dataset(
            provider_info=info_txt,
            contains_human_genetic_sequences=contains_human_genetic_sequences,
            assay_type=canonical_assay_type,
            direct_ancestor_uuids=direct_ancestor_uuids,
            group_uuid=group_uuid,
            description=description,
            is_epic=is_epic,
            priority_project_list=priority_project_list,
            reindex=False,
        )
        return rslt["uuid"]


def populate(row, source_entity, entity_factory, dryrun=False, components=None):
    """
    Build the contents of the newly created dataset using info from the parent
    """
    uuid = row["new_uuid"]
    old_data_path = row["data_path"]
    row["data_path"] = "."

    # Contributors and antibodies should point to the path directly.
    old_paths = []
    for path_index in ["contributors_path", "antibodies_path"]:
        if old_path := row.get(path_index):
            old_path = Path(old_path)
            old_paths.append(old_path)
            row[path_index] = str(Path("extras") / old_path.name)
    print(f"Old paths to copy over {old_paths}")

    # Have to cover two cases
    # 1. Case when non_global is set but there is no global/non_global directories
    # 2. Case when non_global is not set but there are global/non_global directories
    is_shared_upload = {"global", "non_global"} == {
        x.name
        for x in source_entity.full_path.glob("*global")
        if x.is_dir() and x.name in ["global", "non_global"]
    }
    non_global_files = row.get("non_global_files")
    print(f"Is {uuid} part of a shared upload? {is_shared_upload}")
    if non_global_files and not pd.isna(non_global_files):
        print(f"Non global files: {non_global_files}")
        # Catch case 1
        assert (
            is_shared_upload
        ), f"{uuid} has non_global_files specified but missing global or non_global directories"

        # Generate a list of file paths for where non_global_files live in the upload
        # Structure is {source_path: path_passed_in_metadata (destination path relative to root of dataset)}
        non_global_files = {
            source_entity.full_path / "non_global" / Path(x.strip()): Path(x.strip())
            for x in non_global_files.split(";")
            if x.strip()
        }

        # Iterate over source_paths and make sure they exist.
        for non_global_file in non_global_files.keys():
            assert (
                non_global_file.exists()
            ), f"Non global file {non_global_file.as_posix()} does not exist in {source_entity.full_path}"
        row["non_global_files"] = ""
    else:
        # Catch case 2
        assert (
            not is_shared_upload
        ), f"{uuid} has empty non_global_files but has global & non_global directories"

    row_df = pd.DataFrame([row])
    row_df = row_df.drop(columns=["canonical_assay_type", "new_uuid"])

    if dryrun:
        kid_path = Path(SCRATCH_PATH) / uuid
        kid_path.mkdir(0o770, parents=True, exist_ok=True)
        print(f"writing this metadata to {kid_path}:")
        print(row_df)
    else:
        kid_path = Path(entity_factory.get_full_path(uuid))

    row_df.to_csv(kid_path / f"{uuid}-metadata.tsv", header=True, sep="\t", index=False)
    dest_extras_path = kid_path / "extras"

    if components is not None:
        for component in components:
            component_df = pd.read_csv(component.get("metadata-file"), sep="\t")
            component_df_cp = component_df.query(f'data_path=="{old_data_path}"').copy()
            for _, row_component in component_df_cp.iterrows():
                # This loop updates the data_path for the component
                row_component["data_path"] = "."
                old_component_paths = []
                for path_index in ["contributors_path", "antibodies_path"]:
                    if old_component_path := row_component.get(path_index):
                        old_component_path = Path(old_component_path)
                        old_component_paths.append(old_component_path)
                        row_component[path_index] = str(Path("extras") / old_component_path.name)

                print(f"Old component paths to copy over {old_component_paths}")

                copy_contrib_antibodies(
                    dest_extras_path, source_entity, old_component_paths, dryrun
                )

                row_component = pd.DataFrame([row_component])
                row_component.to_csv(
                    kid_path / f"{component.get('assaytype')}-metadata.tsv",
                    header=True,
                    sep="\t",
                    index=False,
                )
    # START REGION - INDEPENDENT OF SHARED/NON-SHARED STATUS
    # This moves extras over to the dataset extras directory
    copy_extras(dest_extras_path, source_entity, dryrun)
    # This copies contrib and antibodies over to dataset
    copy_contrib_antibodies(
        dest_extras_path,
        source_entity,
        old_paths,
        dryrun,
    )
    # END REGION - INDEPENDENT OF SHARED/NON-SHARED STATUS

    if is_shared_upload:
        copy_shared_data(kid_path, source_entity, non_global_files, dryrun)
    else:
        # This moves everything in the source_data_path over to the dataset path
        # So part of non-shared uploads
        copy_data_path(kid_path, source_entity.full_path / old_data_path, dryrun)

    print(f"{old_data_path} -> {uuid} -> full path: {kid_path}")


def copy_shared_data(kid_path, source_entity, non_global_files, dryrun):
    # Copy global files over to dataset directory
    if dryrun:
        print(f"Copy files from {source_entity.full_path / 'global'} to {kid_path}")
    else:
        print(f"Copy files from {source_entity.full_path / 'global'} to {kid_path}")
        copytree(source_entity.full_path / "global", kid_path, dirs_exist_ok=True)
    # Copy over non-global files to dataset directory
    for source_non_global_file, dest_relative_non_global_file in non_global_files.items():
        dest_non_global_file = kid_path / dest_relative_non_global_file
        if dryrun:
            print(f"Copy file from {source_non_global_file} to {dest_non_global_file}")
        else:
            print(f"Copy file from {source_non_global_file} to {dest_non_global_file}")
            dest_non_global_file.parent.mkdir(parents=True, exist_ok=True)
            copy2(source_non_global_file, dest_non_global_file)


def copy_data_path(kid_path, source_data_path, dryrun):
    for elt in source_data_path.glob("*"):
        dst_file = kid_path / elt.name
        if dryrun:
            if dst_file.exists() and dst_file.is_dir():
                for sub_elt in elt.glob("*"):
                    print(f"rename {sub_elt} to {kid_path / elt.name / sub_elt.name}")
                continue
            print(f"rename {elt} to {dst_file}")
        else:
            if dst_file.exists() and dst_file.is_dir():
                for sub_elt in elt.glob("*"):
                    sub_elt.rename(kid_path / elt.name / sub_elt.name)
                continue
            elt.rename(dst_file)


def copy_extras(dest_extras_path, source_entity, dryrun):
    if dest_extras_path.exists():
        assert dest_extras_path.is_dir(), f"{dest_extras_path} is not a directory"
    else:
        source_extras_path = source_entity.full_path / "extras"
        if source_extras_path.exists():
            if dryrun:
                print(f"copy {source_extras_path} to {dest_extras_path}")
            else:
                print(f"copy {source_extras_path} to {dest_extras_path}")
                copytree(source_extras_path, dest_extras_path, dirs_exist_ok=True)
        else:
            if dryrun:
                print(f"creating {dest_extras_path}")
            dest_extras_path.mkdir(0o770)


def copy_contrib_antibodies(dest_extras_path, source_entity, old_paths, dryrun):
    for old_path in old_paths:
        if dryrun:
            print(f"copy {old_path} to {dest_extras_path}")
        else:
            src_path = source_entity.full_path / old_path
            dest_path = dest_extras_path / old_path.name

            if src_path.exists() and not dest_path.exists():
                dest_extras_path.mkdir(parents=True, exist_ok=True)
                copy2(src_path, dest_path)
                print(f"copy {old_path} to {dest_extras_path}")
            else:
                print(
                    f"""Probably already copied/moved {src_path} 
                    to {dest_path} {"it exists" if dest_path.exists() else "missing file"}"""
                )


def apply_special_case_transformations(
    df: pd.DataFrame, parent_assay_type: StrOrListStr
) -> pd.DataFrame:
    """
    Sometimes special case transformations must be applied, for example because the
    validation rules have changed since the upload was originally validated.
    """
    for regex, fun_lst in SPECIAL_CASE_TRANSFORMATIONS:
        if regex.match(str(parent_assay_type)):
            for fun in fun_lst:
                df = df.apply(fun, axis=1, parent_assay_type=parent_assay_type)
    return df


def update_upload_entity(child_uuid_list, source_entity, dryrun=False, verbose=False):
    """
    Change the database entry for the parent Upload now that it has been Reorganized
    """
    if isinstance(source_entity, Upload):
        if dryrun:
            print(f'set status of <{source_entity.uuid}> to "Reorganized"')
            print(f"set <{source_entity.uuid}> dataset_uuids_to_link to {child_uuid_list}")
        else:
            # Set Upload status to "Reorganized"
            # Set links from Upload to split Datasets
            print(f"Setting status of {source_entity.uuid} to 'Reorganized'. Child UUIDs:")
            pprint(child_uuid_list)
            # Update status first, then let's iterate over the child list
            StatusChanger(
                source_entity.uuid,
                source_entity.entity_factory.auth_tok,
                status=Statuses.UPLOAD_REORGANIZED,
                verbose=verbose,
                reindex=False,
            ).update()
            time.sleep(30)
            print(f"{source_entity.uuid} status is Reorganized")

            # Batch update the child uuids
            for chunk in [
                child_uuid_list[i : i + 100] for i in range(0, len(child_uuid_list), 100)
            ]:
                StatusChanger(
                    source_entity.uuid,
                    source_entity.entity_factory.auth_tok,
                    fields_to_overwrite={"dataset_uuids_to_link": list(chunk)},
                    verbose=verbose,
                    reindex=False,
                ).update()
                time.sleep(60)

            for child_uuid_chunk in [
                child_uuid_list[i : i + 10] for i in range(0, len(child_uuid_list), 10)
            ]:
                for uuid in child_uuid_chunk:
                    print(f"Setting status of dataset {uuid} to Submitted")
                    StatusChanger(
                        uuid,
                        source_entity.entity_factory.auth_tok,
                        status=Statuses.DATASET_SUBMITTED,
                        verbose=verbose,
                        reindex=False,
                    ).update()
                    print(
                        f"Reorganized new: {uuid} from Upload: {source_entity.uuid} status is Submitted"
                    )
                time.sleep(30)
    else:
        print(
            f"source entity <{source_entity.uuid}> is not an upload,"
            " so its status was not updated"
        )


def submit_uuid(uuid, entity_factory, dryrun=False):
    """
    Submit the given dataset, causing it to be ingested.
    """
    if dryrun:
        print(f"Not submitting uuid {uuid}.")
        return uuid
    else:
        uuid_entity_to_submit = entity_factory.get(uuid)
        rslt = entity_factory.submit_dataset(
            uuid=uuid,
            contains_human_genetic_sequences=uuid_entity_to_submit.contains_human_genetic_sequences,
            reindex=False,
        )
        time.sleep(10)
        return rslt


def reorganize(source_uuid, **kwargs) -> Union[Tuple, None]:
    """
    Carry out the reorganization.  Parameters and kwargs are:

    source_uuid: the uuid to be reorganized
    kwargs['auth_tok']: auth token
    kwargs['mode']: one of ['stop', 'unstop', 'all']
    kwargs['ingest']: boolean.  Should the split datasets be immediately ingested?
    kwargs['dryrun']: boolean.  If dryrun=True, actions will be printed but no changed
                                to the database or data will be made.
    kwargs['instance']: one of the instances, e.g. 'PROD' or 'DEV'
    kwargs['frozen_df_fname']: path for the tsv file created/read in stop/unstop mode.  It
                               must be formattable as frozen_df_fname.format(index_string)
    kwargs['verbose']: if present and True, increase verbosity of output
    """
    auth_tok = kwargs["auth_tok"]
    mode = kwargs["mode"]
    ingest = kwargs["ingest"]
    dryrun = kwargs["dryrun"]
    instance = kwargs["instance"]
    frozen_df_fname = kwargs["frozen_df_fname"]
    verbose = kwargs.get("verbose", False)
    dag_config = {}
    child_uuid_list = []

    entity_factory = EntityFactory(auth_tok, instance=instance)

    print(f"Decomposing {source_uuid}")
    source_entity = entity_factory.get(source_uuid)
    full_entity = SoftAssayClient(
        list(source_entity.full_path.glob("*metadata.tsv")),
        auth_tok,
    )
    if mode in ["all", "stop"]:
        if hasattr(source_entity, "dataset_type"):
            # ToDo: review this change to dataset_type
            assert isinstance(source_entity.data_types, str)
            source_data_types = source_entity.data_types
        else:
            source_data_types = None
        for src_idx, smf in enumerate([full_entity.primary_assay.get("metadata-file")]):
            source_df = pd.read_csv(smf, sep="\t")
            source_df["canonical_assay_type"] = source_df.apply(
                get_canonical_assay_type,
                axis=1,
                dataset_type=full_entity.primary_assay.get("dataset-type"),
            )

            new_uuids = []
            for df_chunk in [source_df[i : i + 10] for i in range(0, len(source_df), 10)]:
                for df_i in range(len(df_chunk)):
                    df = df_chunk.iloc[df_i]
                    new_uuids.append(
                        create_new_uuid(
                            df,
                            source_entity=source_entity,
                            entity_factory=entity_factory,
                            primary_entity=full_entity.primary_assay,
                            dryrun=dryrun,
                        )
                    )
                time.sleep(30)

            source_df["new_uuid"] = new_uuids

            source_df = apply_special_case_transformations(source_df, source_data_types)
            print(source_df[["data_path", "canonical_assay_type", "new_uuid"]])
            this_frozen_df_fname = frozen_df_fname.format("_" + str(src_idx))
            source_df.to_csv(this_frozen_df_fname, sep="\t", header=True, index=False)
            print(f"wrote {this_frozen_df_fname}")

    if mode == "stop":
        return

    if mode in ["all", "unstop"]:
        dag_config = {"uuid_list": [], "collection_type": ""}
        for src_idx, _ in enumerate([full_entity.primary_assay.get("metadata-file")]):
            this_frozen_df_fname = frozen_df_fname.format("_" + str(src_idx))
            source_df = pd.read_csv(this_frozen_df_fname, sep="\t")
            print(f"read {this_frozen_df_fname}")

            for _, row in source_df.iterrows():
                dag_config["uuid_list"].append(row["new_uuid"])
                child_uuid_list.append(row["new_uuid"])
                populate(
                    row,
                    source_entity,
                    entity_factory,
                    dryrun=dryrun,
                    components=full_entity.assay_components,
                )

        update_upload_entity(child_uuid_list, source_entity, dryrun=dryrun, verbose=verbose)

        if ingest:
            print("Beginning ingestion")
            for uuid in child_uuid_list:
                submit_uuid(uuid, entity_factory, dryrun)
                if not dryrun:
                    while entity_factory.get(uuid).status not in ["QA", "Invalid", "Error"]:
                        time.sleep(30)

    print(json.dumps(dag_config))
    return child_uuid_list, full_entity.is_multiassay, full_entity.is_epic


def create_multiassay_component(
    source_uuid: str,
    auth_tok: str,
    components: list,
    parent_dir: str,
    reindex: bool = True,
) -> None:
    headers = {
        "authorization": "Bearer " + auth_tok,
        "content-type": "application/json",
        "X-Hubmap-Application": "ingest-pipeline",
    }

    response = HttpHook("GET", http_conn_id="entity_api_connection").run(
        endpoint=f"entities/{source_uuid}?exclude=direct_ancestors.files",
        headers=headers,
        extra_options={"check_response": False},
    )
    response.raise_for_status()
    response_json = response.json()
    if "group_uuid" not in response_json:
        print(f"response from GET on entities{source_uuid}:")
        pprint(response_json)
        raise ValueError("entities response did not contain group_uuid")
    parent_group_uuid = response_json["group_uuid"]

    data = {
        "creation_action": "Multi-Assay Split",
        "group_uuid": parent_group_uuid,
        "direct_ancestor_uuids": [source_uuid],
        "datasets": [
            {
                "dataset_link_abs_dir": parent_dir,
                "contains_human_genetic_sequences": component.get("contains-pii"),
                "dataset_type": component.get("dataset-type"),
            }
            for component in components
        ],
    }
    print(f"Data to create components {data}")
    response = HttpHook("POST", http_conn_id="ingest_api_connection").run(
        endpoint=f"datasets/components?reindex={reindex}",
        headers=headers,
        data=json.dumps(data),
    )
    response.raise_for_status()


def reorganize_multiassay(source_uuid, verbose=False, reindex=True, **kwargs) -> None:
    auth_tok = kwargs["auth_tok"]
    instance = kwargs["instance"]

    entity_factory = EntityFactory(auth_tok, instance=instance)
    print(f"Creating components {source_uuid}")

    source_entity = entity_factory.get(source_uuid)
    full_entity = SoftAssayClient(list(source_entity.full_path.glob("*metadata.tsv")), auth_tok)
    # We are NOT passing in the priority project list here. Doesn't seem that it'd be helpful to have that information
    # tied on the components of a multi-assay dataset. (05/21/2025 - Juan Muerto)
    print(f"Assay components: {full_entity.assay_components}")
    create_multiassay_component(
        source_uuid,
        auth_tok,
        full_entity.assay_components,
        str(source_entity.full_path),
        reindex=reindex,
    )
    StatusChanger(
        source_entity.uuid,
        source_entity.entity_factory.auth_tok,
        status=Statuses.DATASET_SUBMITTED,
        verbose=verbose,
        reindex=reindex,
    ).update()
    print(f"{source_entity.uuid} status is Submitted")


def main():
    """
    main
    """
    parser = argparse.ArgumentParser()
    simplified_frozen_df_fname = DEFAULT_FROZEN_DF_FNAME.format("")  # no suffix
    parser.add_argument(
        "uuid", help="input .txt file containing uuids or" " .csv or .tsv file with uuid column"
    )
    parser.add_argument(
        "--stop",
        help="stop after creating child uuids and writing" f" {simplified_frozen_df_fname}",
        action="store_true",
    )
    parser.add_argument(
        "--unstop",
        help="do not create child uuids;" f" read {simplified_frozen_df_fname} and continue",
        action="store_true",
    )
    parser.add_argument(
        "--instance",
        help="instance to use." f" One of {list(ENDPOINTS)} (default %(default)s)",
        default="PROD",
    )
    parser.add_argument(
        "--dryrun",
        help="describe the steps that would be taken but" " do not make changes",
        action="store_true",
    )
    parser.add_argument(
        "--ingest", help="automatically ingest the generated datasets", action="store_true"
    )

    args = parser.parse_args()

    if args.stop and args.unstop:
        parser.error("--stop and --unstop are mutually exclusive")
    if len(args.uuid) == 32:
        try:
            int(args.uuid, base=16)
        except ValueError:
            parser.error(f"{args.uuid} doesn't look like a uuid")
    else:
        parser.error(f"{args.uuid} is the wrong length to be a uuid")
    if args.instance not in ENDPOINTS.keys():
        parser.error(f"{args.instance} is not a known instance")
    source_uuid = args.uuid
    instance = args.instance
    dryrun = args.dryrun
    ingest = args.ingest
    if args.stop:
        mode = "stop"
    elif args.unstop:
        mode = "unstop"
    else:
        mode = "all"

    print(
        """
        WARNING: this program's default behavior creates new datasets and moves
        files around on PROD. Be very sure you know what it does before you run it!
        """
    )
    auth_tok = input("auth_tok: ")

    reorganize(
        source_uuid,
        auth_tok=auth_tok,
        mode=mode,
        ingest=ingest,
        dryrun=dryrun,
        instance=instance,
        frozen_df_fname=DEFAULT_FROZEN_DF_FNAME,
    )


if __name__ == "__main__":
    main()
