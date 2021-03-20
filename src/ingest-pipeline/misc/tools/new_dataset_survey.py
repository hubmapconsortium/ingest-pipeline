#! /usr/bin/env python

import sys
import argparse
import requests
import json
from pprint import pprint
import pandas as pd

from survey import Entity, Dataset, Sample, EntityFactory, is_uuid


def detect_metadatatsv(ds):
    """
    Returns (True, nrecs) if there is a useable metadata.tsv file in the dataset
    top level directory, or (False, 0) otherwise
    """
    for path in ds.full_path.glob('*metadata.tsv'):
        md_df = pd.read_csv(path, sep='\t')
        print(len(md_df), 'assay_type' in md_df.columns)
        if 'assay_type' in md_df.columns:
            return (True, len(md_df))
    return (False, 0)


def main():
    """
    main
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("uuid_txt", help="input files containing uuids")
    parser.add_argument("--out", help="name of the output .tsv file", required=True)
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
        rec = ds.build_rec()
        rec['has_metadata'], rec['n_md_recs'] = detect_metadatatsv(ds)
        if any([uuid in known_uuids for uuid in new_uuids]):
            rec['note'] = 'UUID COLLISION! '
        known_uuids = known_uuids.union(new_uuids)
        out_recs.append(rec)
    out_df = pd.DataFrame(out_recs).rename(columns={'sample_display_doi':'sample_doi',
                                                    'sample_hubmap_display_id':'sample_display_id',
                                                    'qa_child_uuid':'derived_uuid',
                                                    'qa_child_display_doi':'derived_doi',
                                                    'qa_child_data_type':'derived_data_type',
                                                    'qa_child_status':'derived_status'})
    if in_df is not None:
        out_df = out_df.merge(in_df, left_on='uuid', right_on=uuid_key)
    # out_df.to_csv(args.out, sep='\t', index=False,
    #               columns=['uuid', 'group_name', 'display_doi', 'status', 'data_types',
    #                        'has_metadata', 'n_md_recs'])
    out_df.to_csv(args.out, sep='\t', index=False)
    

if __name__ == '__main__':
    main()

