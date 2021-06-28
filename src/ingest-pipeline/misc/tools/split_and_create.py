#! /usr/bin/env python

import sys
import argparse
from pprint import pprint
from datetime import date
from pathlib import Path
from shutil import copytree
import pandas as pd
import numpy as np

from hubmap_commons.globus_groups import get_globus_groups_info

from survey import (Entity, Dataset, Sample, EntityFactory,
                    ROW_SORT_KEYS, column_sorter, is_uuid,
                    parse_text_list)


#SOURCE_PATH = Path("/hive/hubmap/data/protected/University of California San Diego TMC/"
#                   "1b1b6bb2e8d1c91fe7ab411348304822/1b1b6bb2e8d1c91fe7ab411348304822-metadata.tsv")
#SOURCE_UUID = "1b1b6bb2e8d1c91fe7ab411348304822"
SOURCE_UUID = "6e9e4da81edfe046b3acf3b49e7593c0"

def get_canonical_assay_type(row, entity_factory, default_type):
    try:
        rslt = entity_factory.type_client.getAssayType(row['assay_type']).name
    except:
        rslt = default_type
    print(f"{row['assay_type']} -> {rslt}")
    return rslt
    
def create_new_uuid(row, source_entity, entity_factory):
    assay_type = row['canonical_assay_type']
    rec_identifier = row['data_path'].strip('/')
    assert rec_identifier and rec_identifier != '.', 'Bad data_path!'
    title = source_entity.prop_dct['title'] + ' : ' + rec_identifier
    type_info = entity_factory.type_client.getAssayType(assay_type)
    contains_human_genetic_sequences = type_info.contains_pii
    assert (contains_human_genetic_sequences
            == source_entity.prop_dct['contains_human_genetic_sequences'])
    group_uuid = source_entity.prop_dct['group_uuid']
    description = source_entity.prop_dct['description'] + ' : ' + rec_identifier
    sample_id = row['tissue_id']
    print(f"tissue_id is {sample_id}")
    sample_uuid = entity_factory.id_to_uuid(sample_id)
    print(f"tissue uuid is {sample_uuid}")
    direct_ancestor_uuids = [sample_uuid]

    rslt = entity_factory.create_dataset(
        title=title,
        contains_human_genetic_sequences=contains_human_genetic_sequences,
        assay_type=assay_type,
        direct_ancestor_uuids=direct_ancestor_uuids,
        group_uuid=group_uuid,
        description=description
    )
    return rslt['uuid']


def populate(idx, row, source_df, source_entity, entity_factory):
    uuid = row['new_uuid']
    kid_path = Path(entity_factory.get_full_path(uuid))
    old_data_path = row['data_path']
    row['data_path'] = '.'
    canonical_assay_type = row['canonical_assay_type']
    row['assay_type'] = {'SNARE-Seq2-AC': 'SNARE-seq2',
                         'SNARE-Seq2-R': 'SNARE2-RNAseq'}.get(canonical_assay_type,
                                                              canonical_assay_type)
    row_df = pd.DataFrame([row])
    row_df = row_df.drop(columns=['canonical_assay_type', 'new_uuid'])
    row_df.to_csv(kid_path / f'{uuid}-metadata.tsv', header=True, sep='\t', index=False)
    (kid_path / 'extras').mkdir(0o770, exist_ok=True)
    #copytree(source_entity.full_path / 'extras', kid_path / 'extras',
    #         dirs_exist_ok=True)
    print(f"{old_data_path} -> {uuid} -> full path: {kid_path}")
    
    # c_p = row['contributors_path']
    # #if row['contributors_path'] in inv_uuid_map:
    # #    uuid = inv_uuid_map[row['contributors_path']]
    # #elif 'tissue_id' in row and row['tissue_id'] in samp_to_uuid_map:
    # #    uuid = samp_to_uuid_map[row['tissue_id']]
    # #else:
    # uuid = get_uuid(row['data_path'])
    # if not uuid:
    #     print(f'No uuid found for record {idx}')
    #     continue
    # print(f'row {idx} -> {uuid}')
    # uuid_path = build_tree_root / uuid
    # uuid_path.mkdir()
    # path_str = row['contributors_path']
    # if path_str.startswith('/'):  # common error
    #     path_str = path_str[1:]
    # contributors_path = Path(path_str)
    # if 'antibodies_path' in row:
    #     path_str = row['antibodies_path']
    #     if path_str.startswith('/'):  # common error
    #         path_str = path_str[1:]
    #     antibodies_path = Path(path_str)
    #     row['antibodies_path'] = str(Path('extras').joinpath(antibodies_path))
    # else:
    #     antibodies_path = None
    # print(contributors_path.stem)
    # print([k for k in df_d])
    # assert get_true_stem(contributors_path) in df_d, f"Cannot find contributors dataframe {contributors_path}"
    # row['contributors_path'] = str(Path('extras').joinpath(contributors_path))
    # row['data_path'] = '.'
    # for col in metadata_df.columns:
    #     if col.endswith('_datetime'):
    #         row[col] = reformat_datetime(str(row[col]))
    # row_df = pd.DataFrame([row])
    # if 'do_not_save_this_uuid' not in row or not row['do_not_save_this_uuid']:
    #     row_df.to_csv(uuid_path / f'{uuid}-metadata.tsv', header=True, sep='\t', index=False)
    # (uuid_path / 'extras').mkdir()
    # df_d[get_true_stem(contributors_path)].to_csv(uuid_path / row['contributors_path'],
    #                                               header=True, sep='\t', index=False)
    # if antibodies_path:
    #     df = df_d[get_true_stem(antibodies_path)]
    #     fix_antibodies_df(df).to_csv(uuid_path / row['antibodies_path'],
    #                                  header=True, sep='\t', index=False)



def main():
    """
    main
    """
    # parser = argparse.ArgumentParser()
    # parser.add_argument("uuid_txt",
    #                     help="input .txt file containing uuids or .csv or .tsv file with uuid column")
    # parser.add_argument("--out", help="name of the output .tsv file", required=True)
    # parser.add_argument("--notes", action="append",
    #                     help=("merge dataset notes from this csv/tsv file"
    #                           " (may be repeated)."))
    # args = parser.parse_args()
    auth_tok = input('auth_tok: ')
    entity_factory = EntityFactory(auth_tok, instance='PROD')

    source_entity = entity_factory.get(SOURCE_UUID)
    source_metadata_files = [elt for elt in source_entity.full_path.glob('*metadata.tsv')]
    assert len(source_metadata_files) == 1, f'Too many netadata files in {source_entity.full_path}'
    # source_df = pd.read_csv(source_metadata_files[0], sep='\t')
    # assert isinstance(source_entity.data_types, str)
    # source_df['canonical_assay_type'] = source_df.apply(get_canonical_assay_type,
    #                                                     axis=1,
    #                                                     entity_factory=entity_factory,
    #                                                     default_type=source_entity.data_types)
    # source_df['new_uuid'] = source_df.apply(create_new_uuid, axis=1,
    #                                         source_entity=source_entity,
    #                                         entity_factory=entity_factory)
    # print(source_df[['data_path', 'canonical_assay_type', 'new_uuid']])
    # source_df.to_csv('frozen_source_df.tsv', sep='\t', header=True, index=False)
    # sys.exit('done')

    source_df = pd.read_csv('frozen_source_df.tsv', sep='\t')
    for idx, row in source_df.iterrows():
        populate(idx, row, source_df, source_entity, entity_factory)


if __name__ == '__main__':
    main()

