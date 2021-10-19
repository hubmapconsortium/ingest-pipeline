#! /usr/bin/env python

import sys
import argparse
import re
from pprint import pprint
from datetime import date
from pathlib import Path
from shutil import copytree, copy2
from typing import TypeVar, List
import pandas as pd
import numpy as np
import time
import json

from hubmap_commons.globus_groups import get_globus_groups_info

from survey import (Entity, Dataset, Sample, EntityFactory,
                    ROW_SORT_KEYS, column_sorter, is_uuid,
                    parse_text_list, ENDPOINTS)


FROZEN_DF_FNAME = 'frozen_source_df.tsv'
FAKE_UUID_GENERATOR = None
SCRATCH_PATH = '/tmp/split_and_create'

StrOrListStr = TypeVar('StrOrListStr', str, List[str])

#
# The following are used to try to deal with bad assay type information in the original
# upload or the metadata.tsv file, and with new assay types which have not yet been
# deployed to PROD.
#
FALLBACK_ASSAY_TYPE_TRANSLATIONS = {
    #'SNARE-Seq2-AC': 'SNARE-ATACseq2',
    'SNARE-Seq2-AC': 'SNAREseq',
    #'SNARE2-RNAseq': 'SNARE-RNAseq2',
    'SNARE2-RNAseq': 'sciRNAseq',
    'scRNAseq-10xGenomics-v2': 'scRNA-Seq-10x',
}


#
# Some cases need specialized transformations.  For example, when an input upload
# is validated with an earlier version of the validation tests, but will fail if
# re-validated with the current version.  These transformations can be used to
# bring the metadata into compliance with the current validation rules on the fly.
#
def remove_na(row: pd.Series, parent_assay_type: StrOrListStr) -> pd.Series:
    new_row = row.copy()
    key = 'transposition_kit_number'
    if key in row and row[key].lower() == 'na':
        new_row[key] = ''
    return new_row

SEQ_RD_FMT_TEST_RX = re.compile(r'\d+\+\d+\+\d+\+\d+')
def reformat_seq_read(row: pd.Series, parent_assay_type: StrOrListStr) -> pd.Series:
    new_row = row.copy()
    key = 'sequencing_read_format'
    if key in row and SEQ_RD_FMT_TEST_RX.match(row[key]):
        new_row[key] = row[key].replace('+', '/')
    return new_row

def fix_snare_atac_assay_type(row: pd.Series, parent_assay_type: StrOrListStr) -> pd.Series:
    new_row = row.copy()
    key1 = 'assay_type'
    key2 = 'canonical_assay_type'
    if (key1 in row and key2 in row
        and row[key1] == 'SNARE-seq2' and row[key2] == 'SNAREseq'):
        new_row[key2] = 'SNARE-seq2'
    return new_row

SPECIAL_CASE_TRANSFORMATIONS = [
    (re.compile('SNAREseq'), [remove_na, reformat_seq_read, fix_snare_atac_assay_type])
]


def create_fake_uuid_generator():
    """This is used to simulate unique uuids for dryrun executions"""
    count = 0
    while True:
        rslt = 'fakeuuid_%08x'%count
        count += 1
        yield rslt


def get_canonical_assay_type(row, entity_factory, default_type):
    try:
        rslt = entity_factory.type_client.getAssayType(row['assay_type']).name
    except:
        print(f"fallback {row['assay_type']} {default_type}")
        rslt = FALLBACK_ASSAY_TYPE_TRANSLATIONS.get(row['assay_type'], default_type)
    print(f"{row['assay_type']} -> {rslt}")
    return rslt


def create_new_uuid(row, source_entity, entity_factory, dryrun=False):
    global FAKE_UUID_GENERATOR
    canonical_assay_type = row['canonical_assay_type']
    orig_assay_type = row['assay_type']
    rec_identifier = row['data_path'].strip('/')
    assert rec_identifier and rec_identifier != '.', 'Bad data_path!'
    print('prop_dct follows')
    pprint(source_entity.prop_dct)
    title = source_entity.prop_dct['title'] + ' : ' + rec_identifier
    try:
        type_info = entity_factory.type_client.getAssayType(canonical_assay_type)
    except:
        print(f'tried {orig_assay_type}, canoncal version {canonical_assay_type}')
        print(f'options are {[elt for elt in entity_factory.type_client.iterAssayNames()]}')
        type_info = entity_factory.type_client.getAssayType(orig_assay_type)
    contains_human_genetic_sequences = type_info.contains_pii
    # Check consistency in case this is a Dataset, which will have this info
    if 'contains_human_genetic_sequences' in source_entity.prop_dct:
        assert (contains_human_genetic_sequences
                == source_entity.prop_dct['contains_human_genetic_sequences'])
    group_uuid = source_entity.prop_dct['group_uuid']
    if 'description' in source_entity.prop_dct:
        description = source_entity.prop_dct['description'] + ' : ' + rec_identifier
    else:
        description = source_entity.prop_dct['lab_dataset_id'] + ' : ' + rec_identifier
    sample_id = row['tissue_id']
    print(f"tissue_id is {sample_id}")
    sample_uuid = entity_factory.id_to_uuid(sample_id)
    print(f"tissue uuid is {sample_uuid}")
    direct_ancestor_uuids = [sample_uuid]

    if dryrun:
        if FAKE_UUID_GENERATOR is None:
            FAKE_UUID_GENERATOR = create_fake_uuid_generator()
        uuid = FAKE_UUID_GENERATOR.__next__()
        print(f'Not creating uuid {uuid} with assay_type {canonical_assay_type}')
        return uuid
    else:
        rslt = entity_factory.create_dataset(
            title=title,
            contains_human_genetic_sequences=contains_human_genetic_sequences,
            assay_type=canonical_assay_type,
            direct_ancestor_uuids=direct_ancestor_uuids,
            group_uuid=group_uuid,
            description=description
        )
        return rslt['uuid']


def populate(idx, row, source_df, source_entity, entity_factory, dryrun=False):
    uuid = row['new_uuid']
    old_data_path = row['data_path']
    row['data_path'] = '.'
    old_contrib_path = Path(row['contributors_path'])
    new_contrib_path = Path('extras') / old_contrib_path.name
    row['contributors_path'] = str(new_contrib_path)
    if 'antibodies_path' in row:
        old_antibodies_path = Path(row['antibodies_path'])
        new_antibodies_path = Path('extras') / old_antibodies_path.name
        row['antibodies_path'] = str(new_antibodies_path)
    else:
        old_antibodies_path = new_antibodies_path = None
    row['assay_type'] = row['canonical_assay_type']
    row_df = pd.DataFrame([row])
    row_df = row_df.drop(columns=['canonical_assay_type', 'new_uuid'])
    if dryrun:
        kid_path = Path(SCRATCH_PATH) / uuid
        kid_path.mkdir(0o770, parents=True, exist_ok=True)
        print(f'writing this metadata to {kid_path}:')
        print(row_df)
    else:
        kid_path = Path(entity_factory.get_full_path(uuid))
    row_df.to_csv(kid_path / f'{uuid}-metadata.tsv', header=True, sep='\t', index=False)
    extras_path = kid_path / 'extras'
    if extras_path.exists():
        assert extras_path.is_dir(), f'{extras_path} is not a directory'
    else:
        source_extras_path = source_entity.full_path / 'extras'
        if source_extras_path.exists():
            if dryrun:
                print(f'copy {source_extras_path} to {extras_path}')
            else:
                copytree(source_extras_path, extras_path)
        else:
            if dryrun:
                print(f'creating {extras_path}')
            extras_path.mkdir(0o770)
    source_data_path = source_entity.full_path / old_data_path
    for elt in source_data_path.glob('*'):
        if dryrun:
            print(f'rename {elt} to {kid_path / elt.name}')
        else:
            elt.rename(kid_path / elt.name)
    if dryrun:
        print(f'copy {old_contrib_path} to {extras_path}')
    else:
        copy2(source_entity.full_path / old_contrib_path, extras_path)
    if old_antibodies_path is not None:
        if dryrun:
            print(f'copy {old_antibodies_path} to {extras_path}')
        else:
            copy2(source_entity.full_path / old_antibodies_path, extras_path)
    print(f"{old_data_path} -> {uuid} -> full path: {kid_path}")


def apply_special_case_transformations(df: pd.DataFrame, parent_assay_type: StrOrListStr) -> pd.DataFrame:
    """
    Sometimes special case transformations must be applied, for example because the
    valisation rules have changed since the upload was originally validated.
    """
    for regex, fun_lst in SPECIAL_CASE_TRANSFORMATIONS:
        if regex.match(str(parent_assay_type)):
            for fun in fun_lst:
                df = df.apply(fun, axis=1, parent_assay_type=parent_assay_type)
    return df


def submit_uuid(uuid, entity_factory, dryrun=False):
    if dryrun:
        print(f'Not submitting uuid {uuid}.')
        return uuid
    else:
        uuid_entity_to_submit = entity_factory.get(uuid)
        rslt = entity_factory.submit_dataset(
            uuid=uuid,
            contains_human_genetic_sequences=uuid_entity_to_submit.contains_human_genetic_sequences
        )
        return rslt

def main():
    """
    main
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("uuid",
                        help="input .txt file containing uuids or .csv or .tsv file with uuid column")
    parser.add_argument("--stop", help=f"stop after creating child uuids and writing {FROZEN_DF_FNAME}",
                        action="store_true", )
    parser.add_argument("--unstop", help=f"do not create child uuids; read {FROZEN_DF_FNAME} and continue",
                        action="store_true")
    parser.add_argument("--instance",
                        help=f"instance to use. One of {[k for k in ENDPOINTS.keys()]} (default %(default)s)",
                        default = 'PROD')
    parser.add_argument("--dryrun", help="describe the steps that would be taken but do not make changes",
                        action="store_true")
    parser.add_argument("--ingest", help="automatically ingest the generated datasets", action="store_true")

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
        mode = 'stop'
    elif args.unstop:
        mode = 'unstop'
    else:
        mode = 'all'

    print(
        """
        WARNING: this program's default behavior creates new datasets and moves
        files around on PROD. Be very sure you know what it does before you run it!
        """
    )
    auth_tok = input('auth_tok: ')
    entity_factory = EntityFactory(auth_tok, instance=instance)

    print(f'Decomposing {source_uuid}')
    source_entity = entity_factory.get(source_uuid)
    if mode in ['all', 'stop']:
        source_metadata_files = [elt for elt in source_entity.full_path.glob('*metadata.tsv')]
        assert len(source_metadata_files) == 1, f'Too many netadata files in {source_entity.full_path}'
        source_df = pd.read_csv(source_metadata_files[0], sep='\t')
        if hasattr(source_entity, 'data_types'):
            assert isinstance(source_entity.data_types, str)
            source_data_types = source_entity.data_types
        else:
            source_data_types = None
        source_df['canonical_assay_type'] = source_df.apply(get_canonical_assay_type,
                                                            axis=1,
                                                            entity_factory=entity_factory,
                                                            default_type=source_data_types)
        source_df['new_uuid'] = source_df.apply(create_new_uuid, axis=1,
                                                source_entity=source_entity,
                                                entity_factory=entity_factory,
                                                dryrun=dryrun)
        source_df = apply_special_case_transformations(source_df, source_data_types)
        print(source_df[['data_path', 'canonical_assay_type', 'new_uuid']])
        source_df.to_csv(FROZEN_DF_FNAME, sep='\t', header=True, index=False)
        print(f'wrote {FROZEN_DF_FNAME}')

    if mode == 'stop':
        sys.exit('done')

    if mode == 'unstop':
        source_df = pd.read_csv(FROZEN_DF_FNAME, sep='\t')
        print(f'read {FROZEN_DF_FNAME}')

    dag_config = {'uuid_list': [], 'collection_type': ''}

    if mode in ['all', 'unstop']:
        for idx, row in source_df.iterrows():
            dag_config['uuid_list'].append(row['new_uuid'])
            populate(idx, row, source_df, source_entity, entity_factory, dryrun=dryrun)

        if ingest:
            print('Beginning ingestion')
            for uuid in dag_config['uuid_list']:
                submit_uuid(uuid, entity_factory, dryrun)
                if not dryrun:
                    while entity_factory.get(uuid).status not in ['QA', 'Invalid', 'Error']:
                        time.sleep(30)


    print(json.dumps(dag_config))


if __name__ == '__main__':
    main()
