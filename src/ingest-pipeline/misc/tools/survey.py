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
    def __init__(self, prop_dct, entity_factory):
        self.uuid = prop_dct['uuid']
        self.prop_dct = prop_dct
        self.entity_factory = entity_factory
        self.display_doi = prop_dct['display_doi']

    def describe(self, prefix='', file=sys.stdout):
        print(f"{prefix}{self.uuid}: "
              f"{self.display_doi} ",
              file=file)
        if self.kid_dataset_uuids:
            for kid in self.kid_dataset_uuids:
                self.kids[kid].describe(prefix=prefix+'    ', file=file)


class Dataset(Entity):
    def __init__(self, prop_dct, entity_factory):
        super().__init__(prop_dct, entity_factory)
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
        self.donor_uuid = prop_dct['donor']['uuid']

        self._kid_dct = None
        self._parent_dct = None
    
    @property
    def kids(self):
        if self._kid_dct is None:
            self._kid_dct = {uuid: self.entity_factory.get(uuid) for uuid in self.kid_uuids}
        return self._kid_dct
    
    @property
    def parents(self):
        if self._parent_dct is None:
            self._parent_dct = {uuid: self.entity_factory.get(uuid) for uuid in self.parent_uuids}
        return self._parent_dct

    def describe(self, prefix='', file=sys.stdout):
        print(f"{prefix}Dataset {self.uuid}: "
              f"{self.display_doi} "
              f"{self.data_types} "
              f"{self.status}",
              file=file)
        if self.kid_dataset_uuids:
            for kid in self.kid_dataset_uuids:
                self.kids[kid].describe(prefix=prefix+'    ', file=file)

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
        other_parent_uuids = [uuid for uuid in self.parent_uuids if uuid not in self.parent_dataset_uuids]
        assert len(other_parent_uuids) == 1, 'More than one parent?'
        samp = self.entity_factory.get(other_parent_uuids[0])
        assert isinstance(samp, Sample), 'was expecting a sample?'
        rec['sample_display_doi'] = samp.display_doi
        rec['sample_hubmap_display_id'] = samp.hubmap_display_id
        qa_kids = [self.kids[uuid] for uuid in self.kids if self.kids[uuid].status == 'QA']
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
            

class Sample(Entity):
    def __init__(self, prop_dct, entity_factory):
        super().__init__(prop_dct, entity_factory)
        assert prop_dct['entity_type'] == 'Sample', f"uuid {uuid} is a {prop_dct['entity_type']}"
        self.display_doi = prop_dct['display_doi']
        self.donor_display_doi = prop_dct['donor']['display_doi']
        self.hubmap_display_id = prop_dct['hubmap_display_id']
        self.donor_hubmap_display_id = prop_dct['donor']['hubmap_display_id']
        self.donor_uuid = prop_dct['donor']['uuid']

    def describe(self, prefix='', file=sys.stdout):
        print(f"{prefix}Sample {self.uuid}: "
              f"{self.display_doi} "
              f"{self.hubmap_display_id}",
              file=file)


class EntityFactory(object):
    def __init__(self, auth_tok=None):
        assert auth_tok, 'auth_tok is required'
        self.auth_tok = auth_tok
    
    def get(self, uuid):
        """
        Returns an entity of some kind
        """
        data = {'query': {'ids': {'values': [f'{uuid}']}}}
        r = requests.post(f'{SEARCH_URL}/portal/search',
                          data=json.dumps(data),
                          headers={'Authorization': f'Bearer {self.auth_tok}',
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
        entity_type = prop_dct['entity_type']
        if entity_type == 'Dataset':
            return Dataset(prop_dct, self)
        elif entity_type == 'Sample':
            return Sample(prop_dct, self)
        else:
            return Entity(prop_dct, self)


def is_uuid(s):
    return s and len(s) == 32 and all([c in '0123456789abcdef' for c in list(s)])


def robust_find_uuid(s):
    words = s.split('/')
    while words:
        if is_uuid(words[0]):
            return words[0]
        else:
            words = words[1:]
    return None


def get_uuid(rec):
    return robust_find_uuid(rec['data_path'])


def main():
    """
    main
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("metadatatsv")
    parser.add_argument("--out", help="name of the output .tsv file", required=True)
    args = parser.parse_args()
    auth_tok = input('auth_tok: ')
    entity_factory = EntityFactory(auth_tok)
    in_df = pd.read_csv(args.metadatatsv, sep='\t')
    in_df['uuid'] = in_df.apply(get_uuid, axis=1)
    out_recs = []
    for idx, row in in_df.iterrows():
        uuid = row['uuid']
        ds = entity_factory.get(uuid)
        ds.describe()
        out_recs.append(ds.build_rec())
    out_df = pd.DataFrame(out_recs).rename(columns={'sample_display_doi':'sample_doi',
                                                    'sample_hubmap_display_id':'sample_display_id',
                                                    'qa_child_uuid':'derived_uuid',
                                                    'qa_child_display_doi':'derived_doi',
                                                    'qa_child_data_type':'derived_data_type',
                                                    'qa_child_status':'derived_status'})
    out_df.to_csv(args.out, sep='\t', index=False)
    

if __name__ == '__main__':
    main()

