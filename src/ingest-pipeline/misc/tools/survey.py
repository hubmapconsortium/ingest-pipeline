#! /usr/bin/env python

import sys
import argparse
import requests
import json
from pprint import pprint

ENTITY_URL = 'https://entity.api.hubmapconsortium.org'  # no trailing slash
SEARCH_URL = 'https://search.api.hubmapconsortium.org'


def _get_entity_prov(uuid, auth_tok):
    """
    not currently used
    """
    r = requests.get(f'{ENTITY_URL}/entities/{uuid}/provenance',
                     headers={'Authorization': f'Bearer {auth_tok}'})
    if r.status_code >= 300:
        r.raise_for_status()
    pprint(r.json())
    jsn = r.json()
    print(jsn.keys())
    for key in ['agent', 'entity', 'activity']:
        print(f'-----{key}------')
        print(jsn[key].keys())
    print('------ACTIVITIES-----------')
    for elt in jsn['activity']:
        thing = jsn['activity'][elt]
        print(f"{elt}: {thing['hubmap:uuid']} {thing['prov:type']}")
    print('------ENTITIES-------------')
    for elt in jsn['entity']:
        thing = jsn['entity'][elt]
        print(f"{elt}: "
              f"{thing['hubmap:displayDOI']} "
              f"{thing['hubmap:displayIdentifier']} {thing['hubmap:uuid']} "
              f"{thing['prov:label']} {thing['prov:type']}")


class Dataset(object):
    def __init__(self, uuid, auth_tok=None):
        assert auth_tok, 'auth_tok is required'
        data = {'query': {'ids': {'values': [f'{uuid}']}}}
        r = requests.post(f'{SEARCH_URL}/portal/search',
                          data=json.dumps(data),
                          headers={'Authorization': f'Bearer {auth_tok}',
                                   'Content-Type': 'application/json'})
        #print(f'query was {r.request.body}')
        if r.status_code >= 300:
            r.raise_for_status()
        jsn = r.json()
        assert len(jsn['hits']['hits']) == 1, f'More than one hit on uuid {uuid}'
        hit = jsn['hits']['hits'][0]
        assert hit['_id'] == uuid, f"uuid {uuid} gave back uuid {hit['_id']}"
        prop_dct = hit['_source']
        assert prop_dct['uuid'] == uuid, f"uuid {uuid} gave back inner uuid {prop_dct['uuid']}"
        assert prop_dct['entity_type'] == 'Dataset', f"uuid {uuid} is a {prop_dct['entity_type']}"
        
        self.uuid = uuid
        self.status = prop_dct['status']
        self.dag_provenance = prop_dct['metadata']['dag_provenance_list']
        self.parent_uuids = [elt['uuid'] for elt in prop_dct['immediate_ancestors']]
        self.parent_dataset_uuids = [elt['uuid'] for elt in prop_dct['immediate_ancestors']
                                     if elt['entity_type'] == 'Dataset']
        self.kid_uuids = [elt['uuid'] for elt in prop_dct['immediate_descendants']]
        self.kid_dataset_uuids = [elt['uuid'] for elt in prop_dct['immediate_descendants']
                                  if elt['entity_type'] == 'Dataset']
        self.data_types = prop_dct['data_types']
        self.display_doi = prop_dct['display_doi']
        self.donor_uuid = prop_dct['donor']['uuid']
        
        self.kid_datasets = [Dataset(kid_uuid, auth_tok=auth_tok)
                             for kid_uuid in self.kid_dataset_uuids]
            
    
    def describe(self, prefix='', file=sys.stdout):
        print(f"{prefix}{self.uuid}: "
              f"{self.display_doi} "
              f"{self.data_types} "
              f"{self.status}",
              file=file)
        for kid in self.kid_datasets:
            kid.describe(prefix=prefix+'    ', file=file)
        

def main():
    """
    main
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("uuid")
    args = parser.parse_args()
    auth_tok = input('auth_tok: ')
    ds = Dataset(args.uuid, auth_tok)
    ds.describe()
    
    

if __name__ == '__main__':
    main()

