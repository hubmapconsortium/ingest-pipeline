#! /usr/bin/env python

import argparse
import numpy as np
import pandas as pd

from survey import (EntityFactory,
                    ROW_SORT_KEYS, column_sorter, is_uuid,
                    SurveyException)

from new_dataset_survey import (detect_otherdata, detect_metadatatsv,
                                detect_clean_validation_report,
                                get_most_recent_touch, join_notes)


def main():
    """
    main
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "uuid_txt",
        help="input .txt file containing uuids or .csv or .tsv file with uuid column"
    )
    parser.add_argument("--out", help="name of the output .tsv file", required=True)
    parser.add_argument("--notes", action="append",
                        help=("merge dataset notes from this csv/tsv file"
                              " (may be repeated)."))
    args = parser.parse_args()
    auth_tok = input('auth_tok: ')
    entity_factory = EntityFactory(auth_tok)

    uuid_l = []
    if args.uuid_txt.endswith((".csv", ".tsv")):
        in_df = pd.read_csv(args.uuid_txt, engine="python", encoding='utf-8-sig')
        if 'note' in in_df.columns:
            # re-read with better handling for the note string
            in_df = pd.read_csv(args.uuid_txt, engine="python", sep=None,
                                dtype={'note': np.str}, encoding='utf-8-sig')
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
        rec = {}
        try:
            ds = entity_factory.get(uuid)
            ds.describe()
            new_uuids = ds.all_dataset_uuids()
            try:
                rec = ds.build_rec()
                rec['has_metadata'], rec['n_md_recs'] = detect_metadatatsv(ds)
                rec['has_data'] = detect_otherdata(ds)
                rec['validated'] = detect_clean_validation_report(ds)
                try:
                    rec['last_touch'] = get_most_recent_touch(ds)
                except OSError as e:
                    rec['last_touch'] = f'OSError: {e}'
                if any([uuid in known_uuids for uuid in new_uuids]):
                    rec['note'] = 'UUID COLLISION! '
                    print(f'collision on {[uuid for uuid in new_uuids if uuid in known_uuids]}')
                    known_uuids = known_uuids.union(new_uuids)
            except AssertionError as e:
                old_note = rec['note'] if 'note' in rec else ''
                rec['note'] = f'BAD UUID: {e} ' + old_note
                rec['uuid'] = uuid  # just to make sure it is present
        except SurveyException as e:
            print(f'dropping {uuid} because {e}')
            rec['uuid'] = uuid
            rec['note'] = f'not in survey because {e}'
        if rec:
            out_recs.append(rec)
    out_df = pd.DataFrame(out_recs)
    if in_df is not None:
        out_df = out_df.drop_duplicates().merge(in_df.drop_duplicates(),
                                                left_on='uuid', right_on=uuid_key)

    # Some cleanup on out_df before we save it
    drop_list = []
    rename_d = {}
    for col in out_df.columns:
        if col.startswith('derived_'):
            drop_list.append(col)
    if 'group_name' in out_df.columns and 'organization' in out_df.columns:
        drop_list.append('organization')
    if 'hubmap_id_x' in out_df.columns and 'hubmap_id_y' in out_df.columns:
        if (out_df['hubmap_id_y'].isnull()
            | (out_df['hubmap_id_x'] == out_df['hubmap_id_y'])).all():
            drop_list.append('hubmap_id_y')
            rename_d['hubmap_id_x'] = 'hubmap_id'
        else:
            print('ALERT! hubmap_id mismatch mismatch!')
            out_df.to_csv('/tmp/debug_out_df.tsv', sep='\t')
            drop_list.append('hubmap_id_y')
            rename_d['hubmap_id_x'] = 'hubmap_id'
            #raise AssertionError('hubmap_id and hubmap_id do not match?')
    out_df = out_df.drop(drop_list, axis=1)
    if rename_d:
        out_df = out_df.rename(columns=rename_d)

    for notes_file in args.notes or []:
        notes_df = pd.read_csv(notes_file, engine='python', sep=None,
                               dtype={'note': np.str}, encoding='utf-8-sig')
        for elt in ['uuid', 'note']:
            if not elt in notes_df.columns:
                print(f'ERROR: notes file does not contain {elt}, so notes were not merged')
                break
        else:
            out_df = join_notes(out_df, notes_df)

    out_df = out_df.sort_values([key for key in ROW_SORT_KEYS if key in out_df.columns],
                                axis=0)

    out_df.to_csv(args.out, sep='\t', index=False,
                  columns=column_sorter(elt for elt in out_df.columns)
                  )


if __name__ == '__main__':
    main()
