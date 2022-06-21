#!/usr/bin/env python3

# Import modules
import sys
import os
import re
import tempfile
import hubmap_sdk
import pandas as pd
import sys
import subprocess
import datetime
from pprint import pprint
from pathlib import Path
from shutil import rmtree, copy, move

from hubmap_sdk import EntitySdk

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.http_hook import HttpHook
from airflow.exceptions import AirflowException
from airflow.configuration import conf as airflow_conf

from hubmap_operators.common_operators import CreateTmpDirOperator, CleanupTmpDirOperator

import utils
from utils import (
    localized_assert_json_matches_schema as assert_json_matches_schema,
    HMDAG,
    get_tmp_dir_path,
    get_auth_tok,
    find_matching_endpoint,
    get_type_client,
    get_queue_resource,
    get_preserve_scratch_resource,
    )

sys.path.append(airflow_conf.as_dict()['connections']['SRC_PATH']
                .strip("'").strip('"'))

from submodules import (
    hubmapbags_collection_anatomy as collection_anatomy,
    hubmapbags_collection_compound as collection_compound,
    hubmapbags_biosample_substance as biosample_substance,
    hubmapbags_biosample_gene as biosample_gene,
    hubmapbags_assay_type as assay_type,
    hubmapbags_biosample_disease as biosample_disease,
    hubmapbags_anatomy as anatomy,
    hubmapbags_file_describes_collection as file_describes_collection,
    hubmapbags_project_in_project as project_in_project,
    hubmapbags_file_describes_biosample as file_describes_biosample,
    hubmapbags_file_describes_subject as file_describes_subject,
    hubmapbags_biosample_from_subject as biosample_from_subject,
    hubmapbags_subject as subject,
    hubmapbags_subject_in_collection as subject_in_collection,
    hubmapbags_ncbi_taxonomy as ncbi_taxonomy,
    hubmapbags_id_namespace as id_namespace,
    hubmapbags_biosample_in_collection as biosample_in_collection,
    hubmapbags_file_in_collection as file_in_collection,
    hubmapbags_primary_dcc_contact as primary_dcc_contact,
    hubmapbags_biosample as biosample,
    hubmapbags_projects as projects,
    hubmapbags_collection as collection,
    hubmapbags_anatomy as anatomy,
    hubmapbags_file as files,
    hubmapbags_collection_defined_by_project as collection_defined_by_project,
    hubmapbags_collection_disease as collection_disease,
    hubmapbags_collection_gene as collection_gene,
    hubmapbags_collection_phenotype as collection_phenotype,
    hubmapbags_collection_protein as collection_protein,
    hubmapbags_collection_substance as collection_substance,
    hubmapbags_collection_taxonomy as collection_taxonomy,
    hubmapbags_collection_in_collection as collection_in_collection,
    hubmapbags_subject_disease as subject_disease,
    hubmapbags_subject_phenotype as subject_phenotype,
    hubmapbags_subject_race as subject_race,
    hubmapbags_subject_role_taxonomy as subject_role_taxonomy,
    hubmapbags_subject_substance as subject_substance,
    hubmapbags_file_format as file_format,
    hubmapbags_apis as apis,
    hubmapbags_uuids as uuids,
    hubmapbags_utilities as utilities,
)
sys.path.pop()


default_args = {
    'owner': 'hubmap',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 5, 1),
    'email': ['joel.welling@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'xcom_push': True,
    'queue': get_queue_resource('generate_bdbag'),
}

with HMDAG('generate_bdbag',
           schedule_interval=None,
           is_paused_upon_creation=False,
           default_args=default_args,
           user_defined_macros={
               'tmp_dir_path': get_tmp_dir_path,
               'preserve_scratch': get_preserve_scratch_resource('generate_bdbag'),
           },
       ) as dag:


    def get_dataset_full_path(uuid, auth_token):
        """
        Getting the full data path is not yet supported by hubmap_sdk, so we need
        to implement our own REST call.

        uuid: the uuid of the dataset 
        auth_token: the auth token to be used in the query

        returns: dataset full path as a string.
        """
        endpoint = f'datasets/{uuid}/file-system-abs-path'
        http_hook = HttpHook('GET', http_conn_id='ingest_api_connection')
        headers = {
            'authorization' : f'Bearer {auth_token}',
            'content-type' : 'application/json',
            'X-Hubmap-Application' : 'ingest-pipeline',
        }
        try:
            response = http_hook.run(endpoint,
                                     headers=headers,
                                     extra_options={'check_response': False})
            response.raise_for_status()
            path_rslt = response.json()
            print(f'path_rslt for {uuid} follows')
            pprint(path_rslt)
        except HTTPError as e:
            print(f'ERROR: {e} fetching full path for {uuid}')
            if e.response.status_code == codes.unauthorized:
                raise RuntimeError('ingest_api_connection authorization was rejected?')
            else:
                print('benign error')
                return None
        assert 'path' in path_rslt, f'path query for {uuid} produced no path'
        return path_rslt['path']


    def generate_record(uuid, entity_sdk):
        """
        Implicit assumptions made here:
        - ancestors appear in order, with the end closest to the dataset first
        - there is only one donor
        - there is only one organ
        - the donor and the organ will be the last and second-to-last ancestors
        """
        entity = entity_sdk.get_entity_by_id(uuid)
        organs = entity_sdk.get_ancestor_organs(uuid)
        assert len(organs) == 1, f'dataset {uuid} contains more than one organ'
        ancestors = entity_sdk.get_ancestors(uuid)
        largest_sample = None
        donor = None
        for ancestor in ancestors:
            if isinstance(ancestor, hubmap_sdk.donor.Donor):
                donor = ancestor
            elif ancestor not in organs:  # meaning it is cut from an organ
                largest_sample = ancestor
        if not donor:
            raise RuntimeError(f'No donor found for {uuid}')
        if not largest_sample:
            largest_sample = organs[-1]
        return {
            'ds.group_name': entity.group_name,
            'ds.uuid': entity.uuid,
            'ds.hubmap_id': entity.hubmap_id,
            'dataset_uuid': entity.uuid,
            'ds.status': entity.status,
            'ds.data_types': entity.data_types,
            'first_sample_id': largest_sample.hubmap_id,
            'first_sample_uuid': largest_sample.uuid,
            'organ_type': organs[0].organ,
            'organ_id': organs[0].hubmap_id,
            'donor_id': donor.hubmap_id,
            'donor_uuid': donor.uuid,
            'full_path': get_dataset_full_path(uuid, entity_sdk.token)
            }
        
    
    def generate_bdbag(**kwargs):
        entity_token = get_auth_tok(**kwargs)
        entity_connection = HttpHook.get_connection('entity_api_connection')
        entity_url = entity_connection.get_uri()
        entity_url = entity_url.replace('http://https%3a%2f%2f','https://')
        entity_url = entity_url.replace('http://https%3A%2F%2F','https://')
        entity_host = entity_connection.host
        print(f"entity_url as used by EntitySdk: <{entity_url}>")
        entity_sdk = EntitySdk(token=entity_token, service_url=entity_url)
        instance_identifier = find_matching_endpoint(entity_host)
        # This is intended to throw an error if the instance is unknown or not listed
        output_dir = {'PROD' :  '/hive/hubmap/bdbags/auto',
                      'STAGE' : '/hive/hubmap-stage/bdbags/auto',
                      'TEST' :  '/hive/hubmap-test/bdbags/auto',
                      'DEV' :   '/hive/hubmap-dev/bdbags/auto'}[instance_identifier]
        debug = True
        
        tmp_dir_path = get_tmp_dir_path(kwargs['run_id'])

        print('dag_run conf follows:')
        pprint(kwargs['dag_run'].conf)

        try:
            assert_json_matches_schema(kwargs['dag_run'].conf,
                                       'generate_bdbag_metadata_schema.yml')
        except AssertionError as e:
            print('invalid metadata follows:')
            pprint(kwargs['dag_run'].conf)
            raise
        
        compute_uuids = kwargs['dag_run'].conf.get('compute_uuids', False)
        uuid_l = kwargs['dag_run'].conf['uuid_list']
        dbgap_study_id = kwargs['dag_run'].conf.get('dbgap_study_id', None)
        rec_l = [generate_record(uuid, entity_sdk) for uuid in uuid_l]
        columns = ['ds.group_name', 'ds.uuid',
                   'ds.hubmap_id', 'dataset_uuid',
                   'ds.status', 'ds.data_types',
                   'first_sample_id', 'first_sample_uuid',
                   'organ_type', 'organ_id',
                   'donor_id', 'donor_uuid',
                   'full_path']
        assert rec_l, 'no records?'
        for col in columns:
            if col not in rec_l[0]:
                print(f'missing {col}')
        assert all([col in rec_l[0] for col in columns]), 'records are missing expected column?'
        df = pd.DataFrame(rec_l, columns=columns)

        # Switch to working in the scratch directory
        os.chdir(tmp_dir_path)

        for idx, row in df.iterrows():
            status = row['ds.status'].lower()
            data_type = str(row['ds.data_types']).replace('[','').replace(']','').replace('\'','').lower()
            data_provider = row['ds.group_name']
            hubmap_id = row['ds.hubmap_id']
            hubmap_uuid = row['dataset_uuid']
            biosample_id = row['first_sample_id']
            data_directory = row['full_path']
            print('Preparing bag for dataset ' + data_directory )
            computing = data_directory.replace('/','_').replace(' ','_') + '.computing'
            done = '.' + data_directory.replace('/','_').replace(' ','_') + '.done'
            organ_shortcode = row['organ_type']
            organ_id = row['organ_id']
            donor_id = row['donor_id']

            # Should we be dealing with this dataset at all?
            if status != 'published':
                print(f'Ignoring {hubmap_uuid} because it is {status} rather than published')
                continue
            type_for_lookup = row['ds.data_types']
            if isinstance(type_for_lookup, list) and len(type_for_lookup) == 1:
                type_for_lookup = type_for_lookup[0]
            if not get_type_client().getAssayType(type_for_lookup).primary:
                print(f'Ignoring {hubmap_uuid} because type {data_type} is not primary')

            # Create the marker that this dataset's summary is being computed
            print('Creating checkpoint ' + computing)
            with open(computing, 'w') as file:
                pass
                
            print('Checking if output directory exists.')
            output_directory = data_type + '-' + status + '-' + hubmap_uuid

            print('Creating output directory ' + output_directory + '.' )
            try:
                os.mkdir(output_directory)
            except FileExistsError:
                pass
                    
            print('Making file.tsv')
            temp_file = data_directory.replace('/','_').replace(' ','_') + '.pkl'
            
            ignored = files.create_manifest(project_id=data_provider,
                                            assay_type=data_type,
                                            directory=data_directory,
                                            output_directory=output_directory,
                                            dbgap_study_id=dbgap_study_id,
                                            token=entity_token,
                                            dataset_hmid=hubmap_id,
                                            dataset_uuid=hubmap_uuid )

            print('Making biosample.tsv')
            biosample.create_manifest( biosample_id, data_provider, organ_shortcode,
                                       output_directory )

            print('Making biosample_in_collection.tsv')
            biosample_in_collection.create_manifest( biosample_id, hubmap_id, output_directory )

            print('Making project.tsv')
            projects.create_manifest( data_provider, output_directory )

            print('Making project_in_project.tsv')
            project_in_project.create_manifest( data_provider, output_directory )

            print('Making biosample_from_subject.tsv')
            biosample_from_subject.create_manifest( biosample_id, donor_id, output_directory )

            print('Making ncbi_taxonomy.tsv')
            ncbi_taxonomy.create_manifest( output_directory )

            print('Making collection.tsv')
            collection.create_manifest( hubmap_id, output_directory )

            print('Making collection_defined_by_project.tsv')
            collection_defined_by_project.create_manifest( hubmap_id, data_provider, output_directory )

            print('Making file_describes_collection.tsv')
            file_describes_collection.create_manifest( hubmap_id, data_directory, output_directory )

            print('Making dcc.tsv')
            primary_dcc_contact.create_manifest( output_directory )        

            print('Making id_namespace.tsv')
            id_namespace.create_manifest( output_directory )

            print('Making subject.tsv')
            subject.create_manifest( data_provider, donor_id, output_directory )

            print('Making subject_in_collection.tsv')
            subject_in_collection.create_manifest( donor_id, hubmap_id, output_directory )

            print('Making file_in_collection.tsv')
            ignored = file_in_collection.create_manifest( hubmap_id, data_directory, output_directory )

            print('Creating empty files')
            file_describes_subject.create_manifest( output_directory )
            file_describes_biosample.create_manifest( output_directory )
            anatomy.create_manifest( output_directory )
            assay_type.create_manifest( output_directory )
            biosample_disease.create_manifest( output_directory )
            biosample_gene.create_manifest( output_directory )
            biosample_substance.create_manifest( output_directory )
            collection_anatomy.create_manifest( output_directory )
            collection_compound.create_manifest( output_directory )
            collection_disease.create_manifest( output_directory )
            collection_gene.create_manifest( output_directory )
            collection_in_collection.create_manifest( output_directory )
            collection_phenotype.create_manifest( output_directory )
            collection_protein.create_manifest( output_directory )
            collection_substance.create_manifest( output_directory )
            collection_taxonomy.create_manifest( output_directory )
            file_format.create_manifest( output_directory )
            ncbi_taxonomy.create_manifest( output_directory )
            subject_disease.create_manifest( output_directory )
            subject_phenotype.create_manifest( output_directory )
            subject_race.create_manifest( output_directory )
            subject_role_taxonomy.create_manifest( output_directory )
            subject_substance.create_manifest( output_directory )
            file_format.create_manifest( output_directory )
            
            print('Removing checkpoint ' + computing )
            Path(computing).unlink()
                
            print('Counting files')
            n_files = len([name for name in os.listdir( output_directory )
                           if os.path.isfile( os.path.join( output_directory, name ))])
            if n_files != 36:
                print(f'Generated only {n_files} for {hubmap_uuid}; expected 36- dropping this dataset')
                continue

            print('Creating final checkpoint ' + done )
            with open(done, 'w') as file:
                pass

            print('Creating output directory if necessary')
            Path(output_dir).mkdir(exist_ok=True)
            copy(temp_file, output_dir)

            full_output_dir_path = os.path.join(output_dir, output_directory)
            if Path(full_output_dir_path).exists():
                print(f'Removing old version of {full_output_dir_path}')
                rmtree(full_output_dir_path)
            print('Moving directory ' + output_directory + ' to ' + output_dir + '.')
            move( output_directory, output_dir )

            if compute_uuids:
                print('Generating UUIDs via the uuid-api')
                if uuids.should_i_generate_uuids( hubmap_id=hubmap_id,
                                                  filename=temp_file,
                                                  instance=instance_identifier,
                                                  token=entity_token,
                                                  debug=debug):
                    print('UUIDs not found in uuid-api database. Generating UUIDs.')
                    uuids.generate( temp_file, debug=debug )
                else:
                    print('UUIDs found in uuid-api database. Populating local file')
                    uuids.populate_local_file_with_remote_uuids( hubmap_id=hubmap_id,
                                                                 instance=instance_identifier,
                                                                 token=entity_token,
                                                                 debug=debug )
            if debug:
                df=pd.read_pickle( temp_file )
                df.to_csv(temp_file.replace('pkl','tsv'), sep="\t")

        return True
        
    t_generate_bdbag = PythonOperator(
        task_id='generate_bdbag',
        python_callable=generate_bdbag,
        provide_context=True,
        op_kwargs={
            'crypt_auth_tok' : utils.encrypt_tok(airflow_conf.as_dict()
                                                 ['connections']['APP_CLIENT_SECRET']).decode(),
            }
        )
    
    t_create_tmpdir = CreateTmpDirOperator(task_id='create_tmpdir')
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id='cleanup_tmpdir')

    (dag
     >> t_create_tmpdir
     >> t_generate_bdbag
     >> t_cleanup_tmpdir
    )



