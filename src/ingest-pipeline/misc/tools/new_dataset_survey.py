#! /usr/bin/env python

import sys
import argparse
from pprint import pprint
import pandas as pd

from survey import (Entity, Dataset, Sample, EntityFactory,
                    ROW_SORT_KEYS, column_sorter, is_uuid)


def detect_otherdata(ds):
    """
    Returns (True, nrecs) if there is a useable metadata.tsv file in the dataset
    top level directory, or (False, 0) otherwise
    """
    for path in ds.full_path.glob('**/*'):
        if path.is_file() and path.suffix != '.tsv':
            return True
    return False


def detect_metadatatsv(ds):
    """
    Returns True if there are non-tsv files in or below the dataset top level
    directory, or False otherwise.
    """
    for path in ds.full_path.glob('*metadata.tsv'):
        md_df = pd.read_csv(path, sep='\t')
        if 'assay_type' in md_df.columns:
            return (True, len(md_df))
    return (False, 0)


def data_type_resolver(row):
    if isinstance(row["data_types_x"], str) and isinstance(row["data_types_y"], str):
        text_rep = row["data_types_y"]
        if text_rep[0] == '[' and text_rep[-1] == ']':
            text_rep = text_rep[1:-1]
        text_rep = text_rep.strip("'")
        if text_rep == row["data_types_x"]:
            return row["data_types_x"]
    return "????"


def join_notes(df, notes_df):
    print('Joining!')
    df = pd.merge(df, notes_df[['uuid', 'note']], on='uuid', how='left')
    return df


def main():
    """
    main
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("uuid_txt",
                        help="input .txt file containing uuids or .csv or .tsv file with uuid column")
    parser.add_argument("--out", help="name of the output .tsv file", required=True)
    parser.add_argument("--notes", help="merge dataset notes from this csv/tsv file")
    args = parser.parse_args()
    auth_tok = input('auth_tok: ')
    entity_factory = EntityFactory(auth_tok)

    uuid_l = []
    if args.uuid_txt.endswith((".csv", ".tsv")):
        in_df = pd.read_csv(args.uuid_txt)
        if 'uuid' in in_df.columns:
            uuid_key = 'uuid'
        elif 'e.uuid' in in_df.columns:
            uuid_key = 'e.uuid'
        else:
            raise RuntimeError(f'Cannot find uuid column in {args.uuid_txt}')
        for elt in in_df[uuid_key]:
            uuid_l.append(str(elt))
    else:
        in_df = None
        with open(args.uuid_txt) as f:
            for line in f:
                uuid = None
                if is_uuid(line.strip()):
                    uuid = line.strip()
                else:
                    words = line.strip().split()
                    for word in words:
                        a, b = word.split(':')
                        if a.lower() == 'uuid':
                            uuid = b
                            break
                if uuid:
                    uuid_l.append(uuid)
                    print(f'{uuid}')
                else:
                    print(f'cannot find uuid in {line.strip()}')

    out_recs = []
    
    known_uuids = set()
    for uuid in uuid_l:
        ds = entity_factory.get(uuid)
        ds.describe()
        new_uuids = ds.all_uuids()
        try:
            rec = ds.build_rec()
            rec['has_metadata'], rec['n_md_recs'] = detect_metadatatsv(ds)
            rec['has_data'] = detect_otherdata(ds)
            if any([uuid in known_uuids for uuid in new_uuids]):
                rec['note'] = 'UUID COLLISION! '
            known_uuids = known_uuids.union(new_uuids)
            out_recs.append(rec)
        except AssertionError as e:
            print(f"ERROR: DROPPING BAD UUID {uuid}: {e}")
    out_df = pd.DataFrame(out_recs).rename(columns={'sample_display_doi':'sample_doi',
                                                    'sample_hubmap_display_id':'sample_display_id',
                                                    'qa_child_uuid':'derived_uuid',
                                                    'qa_child_display_doi':'derived_doi',
                                                    'qa_child_data_type':'derived_data_type',
                                                    'qa_child_status':'derived_status'})
    if in_df is not None:
        out_df = out_df.merge(in_df, left_on='uuid', right_on=uuid_key)

    # Some cleanup on out_df before we save it
    drop_list = []
    rename_d = {}
    for col in out_df.columns:
        if col.startswith('derived_'):
            drop_list.append(col)
    if 'group_name' in out_df.columns and 'organization' in out_df.columns:
        drop_list.append('organization')
    if 'display_doi' in out_df.columns and 'hubmap_id' in out_df.columns:
        assert (out_df['display_doi'] == out_df['hubmap_id']).all(), 'display_doi and hubmap_id do not match?'
        drop_list.append('display_doi')
    if 'data_types_x' in out_df.columns and 'data_types_y' in out_df.columns:
        out_df['data_types'] = out_df[['data_types_x', 'data_types_y']].apply(data_type_resolver, axis=1)
        drop_list.extend(['data_types_x', 'data_types_y'])
    out_df = out_df.drop(drop_list, axis=1)
    
    if args.notes:
        notes_df = pd.read_csv(args.notes, sep=None)
        for elt in ['uuid', 'note']:
            if not elt in notes_df.columns:
                print(f'ERROR: notes file does not contain {elt}, so notes were not merged')
                break
        else:
            out_df = join_notes(out_df, notes_df)

    out_df = out_df.sort_values(ROW_SORT_KEYS, axis=0)

    out_df.to_csv(args.out, sep='\t', index=False,
                  columns=column_sorter([elt for elt in out_df.columns])
                  )


if __name__ == '__main__':
    main()

