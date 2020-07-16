#! /usr/bin/env python

import sys
import argparse
import requests
import json
from pprint import pprint
import pandas as pd

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


class Entity(object):
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
        self.uuid = uuid
        self.prop_dct = prop_dct        


class Dataset(Entity):
    def __init__(self, uuid, auth_tok=None):
        super().__init__(uuid, auth_tok)
        prop_dct = self.prop_dct
        assert prop_dct['entity_type'] == 'Dataset', f"uuid {uuid} is a {prop_dct['entity_type']}"
        self.status = prop_dct['status']
        if 'metadata' in prop_dct:
            if 'dag_provenance_list' in prop_dct['metadata']:
                dag_prv = prop_dct['metadata']['dag_provenance_list']
            else:
                dag_prv = None
        else:
            dag_prv = None
        self.dag_provenance = dag_prv
        self.parent_uuids = [elt['uuid'] for elt in prop_dct['immediate_ancestors']]
        self.parent_dataset_uuids = [elt['uuid'] for elt in prop_dct['immediate_ancestors']
                                     if elt['entity_type'] == 'Dataset']
        self.kid_uuids = [elt['uuid'] for elt in prop_dct['immediate_descendants']]
        self.kid_dataset_uuids = [elt['uuid'] for elt in prop_dct['immediate_descendants']
                                  if elt['entity_type'] == 'Dataset']
        self.data_types = prop_dct['data_types']
        self.display_doi = prop_dct['display_doi']
        self.donor_uuid = prop_dct['donor']['uuid']

        self.kid_datasets = None
        self.parent_datasets = None


    def init_relatives(self, auth_tok=None):
        assert auth_tok, 'auth_tok is requried'
        self.kid_datasets = [Dataset(kid_uuid, auth_tok=auth_tok)
                             for kid_uuid in self.kid_dataset_uuids]
        self.parent_entities = [Entity(parent_uuid, auth_tok=auth_tok)
                                for parent_uuid in self.parent_uuids]

        op_l = [uuid for uuid in self.parent_uuids if uuid not in self.parent_dataset_uuids]
        print('Other parents: ', op_l)
        if op_l:
            parent_l = [Entity(uuid, auth_tok) for uuid in op_l]
            print('ENTITY TYPE: ', parent_l[0].prop_dct['entity_type'])
            pprint(parent_l[0].prop_dct.keys())
        #parent = Dataset(op_l[0], auth_tok)
        
    
    def describe(self, prefix='', file=sys.stdout):
        print(f"{prefix}{self.uuid}: "
              f"{self.display_doi} "
              f"{self.data_types} "
              f"{self.status}",
              file=file)
        if self.kid_dataset_uuids:
            assert self.kid_datasets, 'init_relatives() has not been called'
            for kid in self.kid_datasets:
                kid.describe(prefix=prefix+'    ', file=file)
    
    def build_rec(self):
        """
        Returns a dict containing:
        
        uuid
        display_doi
        data_types[0]  (verifying there is only 1 entry)
        status
        QA_child.uuid
        QA_child.display_doi
        QA_child.data_types[0]  (verifying there is only 1 entry)
        QA_child.status   (which must be QA)
        note 
        """
        rec = {'uuid': self.uuid, 'display_doi': self.display_doi, 'status': self.status}
        assert len(self.data_types) == 1, f"More than one data_type: {self.data_types}"
        rec['data_type'] = self.data_types[0]
        qa_kids = [kid_ds for kid_ds in self.kid_datasets if kid_ds.status == 'QA']
        if any(qa_kids):
            if len(qa_kids) > 1:
                rec['note'] = 'Multiple QA derived datasets'
            this_kid = qa_kids[0]
            rec['qa_child_uuid'] = this_kid.uuid
            rec['qa_child_display_doi'] = this_kid.display_doi
            rec['qa_child_data_type'] = this_kid.data_types[0]
            rec['qa_child_status'] = 'QA'
        else:
            rec['qa_child_uuid'] = None
            rec['qa_child_display_doi'] = None
            rec['qa_child_data_type'] = None
            rec['qa_child_status'] = None
    
        return rec
            

def get_uuid(rec):
    words = rec['data_path'].split('/')
    uuid = words[1]
    assert len(uuid)==32, f'putative uuid {uuid} has wrong length'
    assert all([c in list('0123456789abcdef') for c in uuid]), f'putative uuid {uuid} has an unexpected character'
    return uuid


def main():
    """
    main
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("metadatatsv")
    args = parser.parse_args()
    auth_tok = input('auth_tok: ')
    in_df = pd.read_csv(args.metadatatsv, sep='\t')
    in_df['uuid'] = in_df.apply(get_uuid, axis=1)
    for idx, row in in_df.iterrows():
        uuid = row['uuid']
        ds = Dataset(uuid, auth_tok=auth_tok)
        ds.init_relatives(auth_tok)
        ds.describe()
        pprint(ds.build_rec())
    #ds = Dataset(args.uuid, auth_tok)
    #ds.describe()
    
    

if __name__ == '__main__':
    main()

