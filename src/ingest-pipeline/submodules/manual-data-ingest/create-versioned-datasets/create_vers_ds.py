import json
import requests
from py2neo import Graph
import traceback
from ingest_props import IngestProps
from hubmap_commons import file_helper

class CreateVersionedDatasets:    
    def __init__(self, property_file_name, old_ds_info_file_name, output_file_name):
        props = IngestProps(property_file_name, required_props = ['uuid.api.url', 'entity.api.url', 'ingest.api.url', 'nexus.token'])
        self.graph = Graph(props.get('neo4j.server'), auth=(props.get('neo4j.username'), props.get('neo4j.password')))
        with open(old_ds_info_file_name) as ds_info:
            self.old_dsets = json.load(ds_info)
        self.out_file_name = output_file_name
        self.uuid_url = file_helper.ensureTrailingSlashURL(props.get('uuid.api.url'))
        self.entity_api_url = file_helper.ensureTrailingSlashURL(props.get('entity.api.url'))
        self.ingest_api_url = file_helper.ensureTrailingSlashURL(props.get('ingest.api.url'))
        self.nexus_token = props.get('nexus.token')
        with open (self.out_file_name, 'a') as f:
            f.write("======newer\tolder\toldest\n")
        
    def create_datasets(self):
        for old_set in self.old_dsets:
            try:
                self.create_versions(old_set)
            except Exception as e:
                with open(self.out_file_name, "a") as f:
                    f.write("Error while creating dataset " )
                traceback.print_exc()
            
    def create_versions(self, old_set):
        orig_uuid = old_set['uuid']
        n_versions = old_set['n_versions']
        if n_versions > 0:
            versions = [orig_uuid]
            ancestor_uuids = self.get_direct_ancestor_uuids(orig_uuid)
            last_version_uuid = orig_uuid
            for _ in range(n_versions):
                new_ds = self.create_versioned_ds(ancestor_uuids, last_version_uuid)
                last_version_uuid = new_ds['uuid']
                versions.append(last_version_uuid)
    
            self.write_version_line(versions)
        
    def get_direct_ancestor_uuids(self, child_uuid):
        anc_url = self.uuid_url + child_uuid + '/ancestors'
        return self.__get_request(anc_url)


    def __get_request(self, url):
        auth_header = {'Authorization': 'Bearer ' + self.nexus_token}                                                             
        resp = requests.get(url, headers=auth_header)
        if resp.status_code == 200:
            return resp.json()
        print(url)
        raise Exception(f"{resp.status_code}: {resp.text}")
            
    def __post_request(self, url, data):
        auth_header = {'Authorization': 'Bearer ' + self.nexus_token}                                                             
        resp = requests.post(url, json=data, headers=auth_header, verify=False)
        if resp.status_code == 200:
            return resp.json()
        print(json.dumps(data))
        print(url)
        raise Exception(f"{resp.status_code}: {resp.text}")
                
    def create_versioned_ds(self, ancestor_uuids, last_version_uuid):
        old_ds = self.__get_request(self.entity_api_url + "entities/" + last_version_uuid)
        description = "Next version of " + last_version_uuid
        if 'description' in old_ds and not old_ds['description'] is None:
            description = description + old_ds['description'] 
        new_ds_data = {}
        new_ds_data['description'] = description
        new_ds_data['data_types'] = old_ds['data_types']
        new_ds_data['title'] = old_ds['title'] + " next version of " + last_version_uuid
        new_ds_data['direct_ancestor_uuids'] = ancestor_uuids
        new_ds_data['contains_human_genetic_sequences'] = old_ds['contains_human_genetic_sequences']
        new_ds_data['group_uuid'] = '5bd084c8-edc2-11e8-802f-0e368f3075e8'
        new_ds_data['previous_revision_uuid'] = last_version_uuid
        return self.__post_request(self.ingest_api_url + "datasets", new_ds_data)
        
    def write_version_line(self, versions):
        tab = ''
        first = True 
        out_line = ''
        for uuid in reversed(versions):
            out_line = out_line + tab + uuid
            if first:
                tab = '\t'
                first = False
        with open (self.out_file_name, 'a') as f:
            f.write(out_line + "\n")
        
creator = CreateVersionedDatasets("versioned_datasets.properties", "previous_datasets.json", "versioned_datasets.tsv")
creator.create_datasets()
        
        