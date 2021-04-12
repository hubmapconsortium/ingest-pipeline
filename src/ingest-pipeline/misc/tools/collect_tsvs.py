#! /usr/bin/env python

"""
Given a list of uuids, this utility collects metadata info from the relevant .tsv files
and stores it together.
"""

import sys
import argparse
from pathlib import Path
from pprint import pprint
import pandas as pd
import pickle

from survey import EntityFactory, Entity, Dataset

# These are used to identify metadata types
METADATA_HAS_THESE_COLS = ['assay_type', 'data_path']
CONTRIB_HAS_THESE_COLS = ['affiliation', 'first_name', 'last_name']
ANTIBDY_HAS_THESE_COLS = ['channel_id', 'antibody_name', 'rr_id']


def get_true_stem(some_path):
    true_stem = Path(some_path.stem)
    while true_stem != Path(true_stem.stem):
        true_stem = Path(true_stem.stem)
    return true_stem


def main():
    """
    main
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("uuid", action="append",
                        help="The uuid of a dataset.  (May be repeated)")
    parser.add_argument("--out", help="name of the output .pkl file", required=True)
    args = parser.parse_args()
    auth_tok = input('auth_tok: ')
    entity_factory = EntityFactory(auth_tok)

    all_md = {}
    assay_to_stem_map = {}
    
    for uuid in args.uuid:
        try:
            ds = entity_factory.get(uuid)
            if isinstance(ds, Dataset):
                path = ds.full_path
                stem = get_true_stem(tsvfile)
                for tsvfile in path.glob('**/*.tsv'):
                    df = pd.read_csv(tsvfile, sep='\t')
                    if all([elt in df.columns for elt in METADATA_HAS_THESE_COLS]):
                        # It's a top-level metadata file
                        df['src_uuid'] = uuid
                        unique_assays = df['assay_type'].unique()
                        assert len(unique_assays) == 1, 'contains multiple assay types'
                        this_assay = unique_assays[0]
                        if this_assay in assay_to_stem_map:
                            old_df = all_md[assay_to_stem_map[this_assay]]
                            all_md[assay_to_stem_map[this_assay]] = old_df.append(df)
                        else:
                            assay_to_stem_map[this_assay] = stem
                            all_md[stem] = df
                    elif (all([elt in df.columns for elt in CONTRIB_HAS_THESE_COLS])
                          or all([elt in df.columns for elt in ANTIBDY_HAS_THESE_COLS])):
                        if stem not in all_md:
                            all_md[stem] = df
                        elif all_md[stem] == df:
                            pass
                        else:
                            raise AssertionError(f'{uuid} uses {stem} to denote a new file'
                                                 ' but that stem has been seen before')
                    else:
                        pass  # There can be tsv files not related to the assay metadata
            else:
                raise AssertionError(f'{uuid} is not the uuid of a dataset')
        except AssertionError as e:
            print(f'skipping bad uuid {uuid}: {e}')
            
    with open(args.out, 'wb') as f:
        pickle.dump((assay_to_stream_map, all_md))
    
    

if __name__ == '__main__':
    main()

