#!/usr/bin/env python3

# Import modules
import sys
import os
import shutil
import re
import tempfile
import hubmap_sdk
import pandas as pd
import sys
import subprocess
import datetime
from pprint import pprint
from hubmap_sdk import EntitySdk


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.http_hook import HttpHook
from airflow.exceptions import AirflowException
from airflow.configuration import conf as airflow_conf

from hubmap_operators.common_operators import CreateTmpDirOperator, CleanupTmpDirOperator

import utils
from utils import (
    HMDAG,
    get_tmp_dir_path,
    get_auth_tok,
    find_matching_endpoint
    )


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
    'queue': utils.map_queue_name('general')
}

with HMDAG('generate_bdbag',
           schedule_interval=None,
           is_paused_upon_creation=False,
           default_args=default_args,
           user_defined_macros={'tmp_dir_path': get_tmp_dir_path}
           ) as dag:

# def __extract_dataset_info_from_db( id, token=None, instance='test', debug=None ):
# 	'''
# 	Helper function that uses the HuBMAP APIs to get dataset info.
# 	'''

# 	j = apis.get_dataset_info( id, token=token, instance=instance )
# 	if j is None:
# 		warnings.warn('Unable to extract data from database.')
# 		return None

# 	hmid = j.get('hubmap_id')
# 	hmuuid = j.get('uuid')
# 	status = j.get('status')
# 	data_types = j.get('data_types')[0]
# 	group_name = j.get('group_name')
# 	group_uuid = j.get('group_uuid')
# 	first_sample_id=j.get('direct_ancestors')[0].get('hubmap_id')
# 	first_sample_uuid=j.get('direct_ancestors')[0].get('uuid')

# 	j = apis.get_provenance_info( id, instance=instance, token=token )
# 	organ_type=j.get('organ_type')[0]
# 	organ_hmid=j.get('organ_hubmap_id')[0]
# 	organ_uuid=j.get('organ_uuid')[0]
# 	donor_hmid=j.get('donor_hubmap_id')[0]
# 	donor_uuid=j.get('donor_uuid')[0]

# 	full_path = os.path.join('/hive/hubmap/data/public',hmuuid)
# 	if not os.path.isdir(full_path):
# 		full_path=os.path.join('/hive/hubmap/data/protected',group_name,hmuuid)

# 	headers = ['ds.group_name', 'ds.uuid', \
# 		'ds.hubmap_id', 'dataset_uuid', \
# 		'ds.status', 'ds.data_types', \
# 		'first_sample_id', 'first_sample_uuid', \
# 		'organ_type', 'organ_id', \
# 		'donor_id', 'donor_uuid', \
# 		'full_path']

# 	df = pd.DataFrame(columns=headers)
# 	df = df.append({'ds.group_name':group_name, \
# 		'ds.uuid':group_uuid, \
# 		'ds.hubmap_id':hmid, \
# 		'dataset_uuid':hmuuid, \
# 		'ds.data_types':data_types, \
# 		'ds.status':status, \
# 		'ds.data_types':data_types, \
# 		'first_sample_id':first_sample_id, \
# 		'first_sample_uuid':first_sample_uuid, \
# 		'organ_type':organ_type, \
# 		'organ_id':organ_hmid, \
# 		'donor_id':donor_hmid, \
# 		'donor_uuid':donor_uuid, \
# 		'full_path':full_path}, ignore_index=True)

# 	return df

    
#     def check_one_uuid(uuid, **kwargs):
#         """
#         Look up information on the given uuid or HuBMAP identifier.
#         Returns:
#         - the uuid, translated from an identifier if necessary
#         - data type(s) of the dataset
#         - local directory full path of the dataset
#         """
#         print(f'Starting uuid {uuid}')
#         my_callable = lambda **kwargs: uuid
#         ds_rslt=utils.pythonop_get_dataset_state(
#             dataset_uuid_callable=my_callable,
#             **kwargs
#         )
#         if not ds_rslt:
#             raise AirflowException(f'Invalid uuid/doi for group: {uuid}')
#         print('ds_rslt:')
#         pprint(ds_rslt)

#         for key in ['status', 'uuid', 'data_types', 'local_directory_full_path',
#                     'metadata']:
#             assert key in ds_rslt, f"Dataset status for {uuid} has no {key}"

#         if not ds_rslt['status'] in ['New', 'Error', 'QA', 'Published']:
#             raise AirflowException(f'Dataset {uuid} is not QA or better')

#         dt = ds_rslt['data_types']
#         if isinstance(dt, str) and dt.startswith('[') and dt.endswith(']'):
#             dt = ast.literal_eval(dt)
#             print(f'parsed dt: {dt}')

#         return (ds_rslt['uuid'], dt, ds_rslt['local_directory_full_path'],
#                 ds_rslt['metadata'])

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
                largest_sample = sample
        if not donor:
            raise RuntimeError(f'No donor found for {uuid}')
        if not largest_sample:
            largest_sample = organs[-1]
        return {
            'ds.group.name': entity.group_name,
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
            'full_path': get_dataset_full_path(uuid, entity_sdk.token)
            }
        
    
    def generate_bdbag(**kwargs):
        '''
	Magic function that (1) computes checksums, (2) generates UUIDs and, (3) builds a big data bag given a HuBMAP ID.
	:param input: A string representing a HuBMAP ID or a TSV file with one line per dataset, e.g. HBM632.JSNP.578
	:type input: string
	:param dbgap_study_id: A string representing a dbGaP study ID, e.g. phs00265
	:type dbgap_study_id: None or string
	:param overwrite: If set to TRUE, then it will overwrite an existing pickle file associated with the HuBMAP ID
	:type overwrite: boolean
	:param copy_output_to: If set, then a copy of the dataframe and the big data bag will be copied to this location
	:type copy_output_to: None or string
	:param token: A token to access HuBMAP resources
	:type token: string or None
	:param instance: Either 'dev', 'test' or 'prod'
	:type instance: string
	:param compute_uuids: If set to TRUE, then 
	:type compute_uuids: boolean
	:param debug: debug flag
	:type debug: boolean
	:rtype: boolean
	'''
        entity_token = get_auth_tok(**kwargs)
        usage_csv = '/hive/hubmap/data/usage-reports/Globus_Usage_Transfer_Detail.csv'
        log_directory = '/hive/users/backups/app001/var/log/gridftp-audit/'
        entity_url = HttpHook.get_connection('entity_api_connection').base_url
        # instance_identifier = find_matching_endpoint(entity_host)
        entity_sdk = EntitySdk(token=entity_token, url=entity_url)
        # This is intended to throw an error if the instance is unknown or not listed
        output_dir = {'PROD' :  '/hive/hubmap/assets/status',
                      'STAGE' : '/hive/hubmap-stage/assets/status',
                      'TEST' :  '/hive/hubmap-test/assets/status',
                      'DEV' :   '/hive/hubmap-dev/assets/status'}[instance_identifier]
        usage_output = f'{output_dir}/usage_report.json'

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
        
        uuid_l = kwargs['dag_run'].conf['uuid_list']
        rec_l = [generate_record(uuid, entity_sdk) for uuid in uuid_l]
        columns = ['ds.group_name', 'ds.uuid',
		   'ds.hubmap_id', 'dataset_uuid',
		   'ds.status', 'ds.data_types',
		   'first_sample_id', 'first_sample_uuid',
		   'organ_type', 'organ_id',
		   'donor_id', 'donor_uuid',
		   'full_path']
        assert rec_l, 'no records?'
        assert all([col in rec_l[0] for col in columns]), 'records are missing expected column?'
        df = pd.DataFrame(rec_l, columns=columns)

	for idx, row in df.iterrows():
	    status = row['ds.status'].lower()
	    data_type = row['ds.data_types'].replace('[','').replace(']','').replace('\'','').lower()
	    data_provider = row['ds.group_name']
	    hubmap_id = row['ds.hubmap_id']
	    hubmap_uuid = row['dataset_uuid']
	    biosample_id = row['first_sample_id']
	    data_directory = row['full_path']
	    print('Preparing bag for dataset ' + data_directory )
	    computing = data_directory.replace('/','_').replace(' ','_') + '.computing'
	    done = '.' + data_directory.replace('/','_').replace(' ','_') + '.done'
	    broken = '.' + data_directory.replace('/','_').replace(' ','_') + '.broken'
	    organ_shortcode = row['organ_type']
	    organ_id = row['organ_id']
	    donor_id = row['donor_id']

	    if overwrite:
		print('Erasing old checkpoint. Re-computing checksums.')
		if Path(done).exists():
		    Path(done).unlink()

	    if Path(done).exists():
		print('Checkpoint found. Avoiding computation. To re-compute erase file ' + done)
	    elif Path(computing).exists():
		print('Computing checkpoint found. Avoiding computation since another process is building this bag.')
	    else:
		with open(computing, 'w') as file:
		    pass
                
		print('Creating checkpoint ' + computing)
                
		if status == 'new':  # shouldn't this be status != 'published'?
		    print('Dataset is not published. Aborting computation.')
		    return
                
		print('Checking if output directory exists.')
		output_directory = data_type + '-' + status + '-' + dataset['dataset_uuid']
                
		print('Creating output directory ' + output_directory + '.' )
		if Path(output_directory).exists() and Path(output_directory).is_dir():
		    print('Output directory found. Removing old copy.')
		    rmtree(output_directory)
		    os.mkdir(output_directory)
		else:
		    print('Output directory does not exist. Creating directory.')
		    os.mkdir(output_directory)
                    
		print('Making file.tsv')
		temp_file = data_directory.replace('/','_').replace(' ','_') + '.pkl'
                    
		if overwrite:
		    print('Removing precomputed checksums')
		    if Path(temp_file).exists():
			Path(temp_file).unlink()
		answer = files.create_manifest(project_id=data_provider,
				               assay_type=data_type,
				               directory=data_directory,
				               output_directory=output_directory,
				               dbgap_study_id=dbgap_study_id,
				               token=token,
				               dataset_hmid=hubmap_id,
				               dataset_uuid=hubmap_uuid )

		print('Making biosample.tsv')
		biosample.create_manifest( biosample_id, data_provider, organ_shortcode, output_directory )

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
		answer = file_in_collection.create_manifest( hubmap_id, data_directory, output_directory )

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
                
		print('Creating final checkpoint ' + done )
		if __get_number_of_files( output_directory ) == 36:
		    with open(done, 'w') as file:
		        pass
		else:
		    warnings.warn('Wrong number of output files. Labeling dataset as broken.')
		    with open(broken, 'w') as file:
			pass

		if copy_output_to is not None:
		    print('Checking if output directory destination exists')
		    if Path(copy_output_to).exists() and Path(copy_output_to).is_dir():
			print('Copying file ' + temp_file + ' to ' + copy_output_to + '.')
			try:
			    copy( temp_file, copy_output_to )
			except:
			    print('Unable to copy file to destination. Check permissions.')

			print('Moving directory ' + output_directory + ' to ' + copy_output_to + '.')
			try:
			    if Path(os.path.join(copy_output_to, output_directory)).exists():
				rmtree(os.path.join(copy_output_to, output_directory))
				move( output_directory, copy_output_to )
			except Exception as e:
			    print('Unable to move folder to destination. Check permissions.')
			    print(e)
		    else:
			warnings.warn('Output directory ' + copy_output_to + ' does not exist. '
                                      'Not copying results to destination.')

		if compute_uuids:
		    print('Generating UUIDs via the uuid-api')
		    if uuids.should_i_generate_uuids( hubmap_id=id,
					              filename=temp_file,
					              instance=instance,
					              token=token,
					              debug=debug):
			print('UUIDs not found in uuid-api database. Generating UUIDs.')
			uuids.generate( temp_file, debug=debug )
		    else:
			print('UUIDs found in uuid-api database. Populating local file')
			uuids.populate_local_file_with_remote_uuids( hubmap_id, \
						                     instance=instance, \
						                     token=token, \
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
     >> t_build_report
     >> t_cleanup_tmpdir
     )



