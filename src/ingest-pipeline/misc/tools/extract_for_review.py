#! /usr/bin/env python

import argparse

import pandas as pd

from survey import (EntityFactory, is_uuid, SurveyException, column_sorter)


def main():
    """
    main
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("uuid_txt",
                        help="input .txt file containing uuids or .csv or .tsv file with uuid column")
    parser.add_argument("--out", help="name of the output .tsv file", required=True)
    args = parser.parse_args()
    auth_tok = input('auth_tok: ')
    entity_factory = EntityFactory(auth_tok)

    uuid_l = []
    if args.uuid_txt.endswith((".csv", ".tsv")):
        in_df = pd.read_csv(args.uuid_txt, engine="python", sep=None, encoding='utf-8-sig')
        if 'uuid' in in_df.columns:
            uuid_key = 'uuid'
        elif 'e.uuid' in in_df.columns:
            uuid_key = 'e.uuid'
        else:
            raise RuntimeError(f'Cannot find uuid column in {args.uuid_txt}')
        for elt in in_df[uuid_key]:
            uuid_l.append(str(elt))
    else:
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
    for uuid in uuid_l:
        rec = []
        try:
            ds = entity_factory.get(uuid)
            if ds.parents is not None:
                for parent_uuid, dataset in ds.parents.items():
                    data = {'derived_dataset_uuid': ds.uuid,
                            'derived_hubmap_id': ds.hubmap_id,
                            'derived_portal_url': 'https://portal.hubmapconsortium.org/browse/dataset/%s' % ds.uuid,
                            'primary_dataset_uuid': parent_uuid,
                            'primary_hubmap_id': dataset.hubmap_id,
                            'primary_dataset_uscs/name': dataset.prop_dct.get('lab_dataset_id', None),
                            'primary_portal_url': 'https://portal.hubmapconsortium.org/browse/dataset/%s' % parent_uuid}
                    rec.append(data)
        except SurveyException as e:
            print(f'dropping {uuid} because {e}')
            rec.append({'derived_dataset_uuid': uuid,
                        'derived_hubmap_id': f'not in survey because {e}',
                        'derived_portal_url': 'N/A',
                        'primary_dataset_id': 'N/A',
                        'primary_hubmap_id': 'N/A',
                        'primary_dataset_uscs/name': 'N/A',
                        'primary_portal_url': 'N/A'})
        if rec:
            out_recs.append(rec)
    formated_rec = [[x.get('derived_dataset_uuid'), x.get('derived_hubmap_id'), x.get('derived_portal_url'),
                     x.get('primary_dataset_uuid'), x.get('primary_hubmap_id'), x.get('primary_dataset_uscs/name'),
                     x.get('primary_portal_url')] for i in out_recs for x in i]
    out_df = pd.DataFrame(formated_rec, columns=['derived_dataset_uuid', 'derived_hubmap_id', 'derived_portal_url',
                                                 'primary_dataset_uuid', 'primary_hubmap_id', 'primary-dataset_uscs/name',
                                                 'primary_portal_url'])
    out_df.to_csv(args.out, sep='\t', index=False)


if __name__ == '__main__':
    main()
