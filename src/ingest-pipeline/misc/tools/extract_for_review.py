#! /usr/bin/env python

import argparse

import pandas as pd

from survey import (EntityFactory, is_uuid, SurveyException)


def main():
    """
    Outputs tabular data to aid TMCs reviewing datasets for publication.
    Inputs a list of UUIDs
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('uuid_txt',
                        help="input .csv or .tsv file with uuid column")
    parser.add_argument('--out', help="name of the output .tsv file", required=True)
    args = parser.parse_args()
    auth_token = input("auth_token: ")
    entity_factory = EntityFactory(auth_token)

    uuid_list = []
    if args.uuid_txt.endswith(('.csv', '.tsv')):
        in_uuids = pd.read_csv(args.uuid_txt, engine='python', sep=None, encoding='utf-8-sig')
        if 'uuid' in in_uuids.columns:
            uuid_key = 'uuid'
        elif 'e.uuid' in in_uuids.columns:
            uuid_key = 'e.uuid'
        else:
            raise RuntimeError(f"Cannot find uuid column in {args.uuid_txt}")
        for uuid in in_uuids[uuid_key]:
            uuid_list.append(str(uuid))
    else:
        print("Invalid file extension, please use .csv or .tsv")
    out_results = []
    for uuid in uuid_list:
        result_list = []
        try:
            dataset = entity_factory.get(uuid)
            if dataset.parents is not None:
                for parent_uuid, _dataset in dataset.parents.items():
                    data = {'derived_dataset_uuid': dataset.uuid,
                            'derived_hubmap_id': dataset.hubmap_id,
                            'derived_portal_url': 'https://portal.hubmapconsortium.org/browse/dataset/%s' % dataset.uuid,
                            'primary_dataset_uuid': parent_uuid,
                            'primary_hubmap_id': _dataset.hubmap_id,
                            'primary_dataset_uscs/name': _dataset.prop_dct.get('lab_dataset_id', None),
                            'primary_portal_url': 'https://portal.hubmapconsortium.org/browse/dataset/%s' % parent_uuid}
                    result_list.append(data)
        except SurveyException as e:
            print(f"dropping {uuid} because {e}")
            result_list.append({'derived_dataset_uuid': uuid,
                                'derived_hubmap_id': f'not in survey because {e}',
                                'derived_portal_url': 'N/A',
                                'primary_dataset_id': 'N/A',
                                'primary_hubmap_id': 'N/A',
                                'primary_dataset_uscs/name': 'N/A',
                                'primary_portal_url': 'N/A'})
        if result_list:
            out_results.append(result_list)
    formated_results = [[x.get('derived_dataset_uuid'), x.get('derived_hubmap_id'), x.get('derived_portal_url'),
                         x.get('primary_dataset_uuid'), x.get('primary_hubmap_id'), x.get('primary_dataset_uscs/name'),
                         x.get('primary_portal_url')] for i in out_results for x in i]
    out_dataframe = pd.DataFrame(formated_results,
                                 columns=['derived_dataset_uuid', 'derived_hubmap_id', 'derived_portal_url',
                                          'primary_dataset_uuid', 'primary_hubmap_id', 'primary-dataset_uscs/name',
                                          'primary_portal_url'])
    out_dataframe.to_csv(args.out, sep='\t', index=False)


if __name__ == '__main__':
    main()
