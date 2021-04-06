#! /usr/bin/env python

import sys
import argparse
import requests
import json
from pathlib import Path
from pprint import pprint
import pandas as pd

ENTITY_URL = 'https://entity.api.hubmapconsortium.org'  # no trailing slash
SEARCH_URL = 'https://search.api.hubmapconsortium.org'

#
# Large negative numbers move columns left, large positive numbers move them right.
# Columns for which no weight is given end up with weight 0, so they sort to the
# middle alphabetically.
#
COLUMN_SORT_WEIGHTS = {
    'note':10,
    'n_md_recs': 9,
    'has_metadata': 8,
    'has_data': 7,
    'group_name': -10,
    'data_types': -9,
    'uuid': -8,
    'hubmap_id': -7,
}


#
# Column labels to be used as keys in sorting rows
#
ROW_SORT_KEYS = ['group_name', 'data_types', 'uuid']


def column_sorter(col_l):
    sort_me = [((COLUMN_SORT_WEIGHTS[key] if key in COLUMN_SORT_WEIGHTS else 0), key) for key in col_l]
    return [key for wt, key in sorted(sort_me)]


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


class SplitTree(object):
    def __init__(self):
        self.root = {}
    def add(self, s):
        words = s.strip().split('-')
        here = self.root
        while words:
            w0 = words.pop(0)
            if w0 not in here:
                here[w0] = {}
            here = here[w0]
    def _inner_dump(self, dct, prefix):
        for elt in dct:
            print(f'{prefix}{elt}:')
            self._inner_dump(dct[elt], prefix+'    ')
    def dump(self):
        self._inner_dump(self.root, '')
    def _inner_str(self, dct, prefix=''):
        k_l = sorted(dct)
        if k_l:
            #print(f'{prefix}k_l: {k_l}')
            if len(k_l) == 1:
                #print(f'{prefix}point 2')
                next_term = self._inner_str(dct[k_l[0]],prefix=prefix+'  ')
                rslt = k_l[0]
                if next_term:
                    rslt += '-' + next_term
                #print(f'{prefix}--> {rslt}')
                return rslt
            else:
                kid_l = [self._inner_str(dct[k],prefix=prefix+'  ') for k in k_l]
                if all([k == '' for k in kid_l]):
                    #print(prefix+'point 3b')
                    s = ','.join(k_l)
                else:
                    #print(prefix+'point 3c')
                    s = ','.join([f'{a}-{b}' for a,b in zip(k_l,kid_l)])
                rslt = f'[{s}]'
                #print(f'{prefix}--> {rslt}')
                return rslt
        else:
            return ''
    def __str__(self):
        return self._inner_str(self.root)


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

    def all_uuids(self):
        """
        Returns a list of unique UUIDs associated with this entity
        """
        return [self.uuid]


class Dataset(Entity):
    def __init__(self, prop_dct, entity_factory):
        super().__init__(prop_dct, entity_factory)
        assert prop_dct['entity_type'] == 'Dataset', f"uuid {uuid} is a {prop_dct['entity_type']}"
        self.status = prop_dct['status']
        if 'metadata' in prop_dct and prop_dct['metadata']:
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
        self.data_types = prop_dct['data_types'] if 'data_types' in prop_dct else []
        self.donor_uuid = prop_dct['donor']['uuid']
        self.group_name = prop_dct['group_name']
        c_h_g = prop_dct['contains_human_genetic_sequences']
        if isinstance(c_h_g, str):
            c_h_g = (c_h_g.lower() in ['yes', 'true'])
        self.contains_human_genetic_sequences = c_h_g
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

    @property
    def full_path(self):
        assert self.status == 'New', f'full_path is not yet implemented for {self.status} files'
        if self.contains_human_genetic_sequences:
            return Path('/hive/hubmap/data/protected') / self.group_name / self.uuid
        else:
            return Path('/hive/hubmap/data/consortium') / self.group_name / self.uuid

    def describe(self, prefix='', file=sys.stdout):
        print(f"{prefix}Dataset {self.uuid}: "
              f"{self.display_doi} "
              f"{self.data_types} "
              f"{self.status}",
              file=file)
        if self.kid_dataset_uuids:
            for kid in self.kid_dataset_uuids:
                self.kids[kid].describe(prefix=prefix+'    ', file=file)


    def build_rec(self, include_all_children=False):
        """
        Returns a dict containing:
        
        uuid
        display_doi
        data_types[0]  (verifying there is only 1 entry)
        status
        QA_child.uuid
        QA_child.display_doi
        QA_child.data_types[0]  (verifying there is only 1 entry)
        QA_child.status   (which must be QA or Published)
        note 

        If include_all_children=True, all child datasets are included rather
        than just those that are QA or Published.
        """
        rec = {'uuid': self.uuid, 'display_doi': self.display_doi, 'status': self.status,
               'group_name': self.group_name}
        if not self.data_types:
            rec['data_types'] = "[]"
        elif len(self.data_types) == 1:
            rec['data_types'] = self.data_types[0]
        else:
            rec['data_types'] = f"[{','.join(self.data_types)}]"
        other_parent_uuids = [uuid for uuid in self.parent_uuids if uuid not in self.parent_dataset_uuids]
        assert other_parent_uuids, 'No parents?'
        s_t = SplitTree()
        for p_uuid in other_parent_uuids:
            samp = self.entity_factory.get(p_uuid)
            assert isinstance(samp, Sample), 'was expecting a sample?'
            s_t.add(samp.hubmap_display_id)
        rec['sample_hubmap_display_id'] = str(s_t)
        if len(other_parent_uuids) == 1:
            rec['sample_display_doi'] = samp.display_doi
        else:
            rec['sample_display_doi'] = 'multiple'
        if include_all_children:
            filtered_kids = [self.kids[uuid] for uuid in self.kids]
            uuid_hdr, doi_hdr, data_type_hdr, status_hdr, note_note = ('child_uuid', 'child_display_doi',
                                                                       'child_data_type', 'child_status',
                                                                       'Multiple derived datasets')
        else:
            filtered_kids = [self.kids[uuid] for uuid in self.kids if self.kids[uuid].status in ['QA', 'Published']]
            uuid_hdr, doi_hdr, data_type_hdr, status_hdr, note_note = ('qa_child_uuid', 'qa_child_display_doi',
                                                                       'qa_child_data_type', 'qa_child_status',
                                                                       'Multiple QA derived datasets')
        if any(filtered_kids):
            rec['note'] = note_note if len(filtered_kids) > 1 else ''
            this_kid = filtered_kids[0]
            rec[uuid_hdr] = this_kid.uuid
            rec[doi_hdr] = this_kid.display_doi
            rec[data_type_hdr] = this_kid.data_types[0]
            rec[status_hdr] = this_kid.status
        else:
            for key in [uuid_hdr, doi_hdr, data_type_hdr, status_hdr]:
                rec[key] = None
            rec['note'] = ''
    
        return rec
            
    def all_uuids(self):
        """
        Returns a list of unique UUIDs associated with this entity
        """
        return super().all_uuids() + self.kid_uuids + self.parent_uuids
    

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
              f"{self.hubmap_display_id} "
              f"{self.donor_hubmap_display_id}",
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
    raise RuntimeError(f'No uuid found in {s}')


def get_uuid(rec):
    return robust_find_uuid(rec['data_path'])


def main():
    """
    main
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("metadatatsv", help="input .tsv or .xlsx file, or a list of uuids in a .txt file")
    parser.add_argument("--out", help="name of the output .tsv file", required=True)
    parser.add_argument("--include_all_children", action="store_true",
                        help="include all children, not just those in the QA or Published states")
    args = parser.parse_args()
    auth_tok = input('auth_tok: ')
    entity_factory = EntityFactory(auth_tok)
    if args.metadatatsv.endswith('.tsv'):
        in_df = pd.read_csv(args.metadatatsv, sep='\t')
    elif args.metadatatsv.endswith('.xlsx'):
        in_df = pd.read_excel(args.metadatatsv)
    elif args.metadatatsv.endswith('.txt'):
        # a list of bare uuids
        recs = []
        for line in open(args.metadatatsv):
            assert is_uuid(line.strip()), f'text file {args.metadatatsv} contains non-uuid {line.strip}'
            recs.append({'data_path': line.strip()})
        in_df = pd.DataFrame(recs)
    else:
        raise RuntimeError('Unrecognized input file format')
    in_df['uuid'] = in_df.apply(get_uuid, axis=1)
    out_recs = []
    
    known_uuids = set()
    for idx, row in in_df.iterrows():
        uuid = row['uuid']
        ds = entity_factory.get(uuid)
        ds.describe()
        new_uuids = ds.all_uuids()
        try:
            rec = ds.build_rec(include_all_children=args.include_all_children)
            if any([uuid in known_uuids for uuid in new_uuids]):
                old_note = rec['note'] if 'note' in rec else ''
                rec['note'] = 'UUID COLLISION! ' + old_note
            known_uuids = known_uuids.union(new_uuids)
            out_recs.append(rec)
        except AssertionError as e:
            print(f"ERROR: DROPPING BAD UUID {uuid}: {e}")
    out_df = pd.DataFrame(out_recs).rename(columns={'sample_display_doi':'sample_doi',
                                                    'sample_hubmap_display_id':'sample_display_id',
                                                    'qa_child_uuid':'derived_uuid',
                                                    'child_uuid':'derived_uuid',
                                                    'qa_child_display_doi':'derived_doi',
                                                    'child_display_doi':'derived_doi',
                                                    'qa_child_data_type':'derived_data_type',
                                                    'child_data_type':'derived_data_type',
                                                    'qa_child_status':'derived_status',
                                                    'child_status':'derived_status'})
    out_df = out_df.sort_values(ROW_SORT_KEYS, axis=0)
    out_df.to_csv(args.out, sep='\t', index=False,
                  columns=column_sorter([elt for elt in out_df.columns])
                  )
    

if __name__ == '__main__':
    main()

