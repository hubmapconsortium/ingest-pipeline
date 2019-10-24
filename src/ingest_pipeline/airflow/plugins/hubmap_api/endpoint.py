"""
Based heavily on https://github.com/airflow-plugins/airflow_api_plugin
"""

import json
import os
import logging

from werkzeug.exceptions import HTTPException, NotFound 

from flask import Blueprint, current_app, send_from_directory, abort, escape, request, Response
from airflow import settings
from airflow.exceptions import AirflowException, AirflowConfigException
from airflow.www.app import csrf
from airflow.models import DagBag, DagRun

from hubmap_api.manager import blueprint as api_bp
from hubmap_api.manager import show_template

LOGGER = logging.getLogger(__name__)

@api_bp.route('/test')
def show_test():
    return show_template('generic.html', title='Test', content='This is a test')

 
class HubmapInputException(Exception):
    pass
 
class HubmapApiResponse:
 
    def __init__(self):
        pass
 
    STATUS_OK = 200
    STATUS_BAD_REQUEST = 400
    STATUS_UNAUTHORIZED = 401
    STATUS_NOT_FOUND = 404
    STATUS_SERVER_ERROR = 500
 
    @staticmethod
    def standard_response(status, payload):
        json_data = json.dumps({
            'response': payload
        })
        resp = Response(json_data, status=status, mimetype='application/json')
        return resp
 
    @staticmethod
    def success(payload):
        return HubmapApiResponse.standard_response(HubmapApiResponse.STATUS_OK, payload)
 
    @staticmethod
    def error(status, error):
        return HubmapApiResponse.standard_response(status, {
            'error': error
        })
 
    @staticmethod
    def bad_request(error):
        return HubmapApiResponse.error(HubmapApiResponse.STATUS_BAD_REQUEST, error)
 
    @staticmethod
    def not_found(error='Resource not found'):
        return HubmapApiResponse.error(HubmapApiResponse.STATUS_NOT_FOUND, error)
 
    @staticmethod
    def unauthorized(error='Not authorized to access this resource'):
        return HubmapApiResponse.error(HubmapApiResponse.STATUS_UNAUTHORIZED, error)
 
    @staticmethod
    def server_error(error='An unexpected problem occurred'):
        return HubmapApiResponse.error(HubmapApiResponse.STATUS_SERVER_ERROR, error)
 
@api_bp.before_request
def verify_authentication():
    authorization = request.headers.get('authorization')
    try:
        api_auth_key = settings.conf.get('HUBMAP_API_PLUGIN', 'HUBMAP_API_AUTH')
    except AirflowConfigException:
        return
 
    if authorization != api_auth_key:
        return HubmapApiResponse.unauthorized("You are not authorized to use this resource")
 
 
def format_dag_run(dag_run):
    return {
        'run_id': dag_run.run_id,
        'dag_id': dag_run.dag_id,
        'state': dag_run.get_state(),
        'start_date': (None if not dag_run.start_date else str(dag_run.start_date)),
        'end_date': (None if not dag_run.end_date else str(dag_run.end_date)),
        'external_trigger': dag_run.external_trigger,
        'execution_date': str(dag_run.execution_date)
    }


def find_dag_runs(session, dag_id, dag_run_id, execution_date):
    qry = session.query(DagRun)
    qry = qry.filter(DagRun.dag_id == dag_id)
    qry = qry.filter(or_(DagRun.run_id == dag_run_id, DagRun.execution_date == execution_date))

    return qry.order_by(DagRun.execution_date).all()

@api_bp.route('/dags', methods=['GET'])
def dags_index():
    dagbag = DagBag('dags')
    dags = []
    for dag_id in dagbag.dags:
        payload = {
            'dag_id': dag_id,
            'full_path': None,
            'is_active': False,
            'last_execution': None,
        }

        dag = dagbag.get_dag(dag_id)

        if dag:
            payload['full_path'] = dag.full_filepath
            payload['is_active'] = (not dag.is_paused)
            payload['last_execution'] = str(dag.latest_execution_date)

        dags.append(payload)

    return HubmapApiResponse.success({'dags': dags})


def _get_required_string(data, st):
    """
    Return data[st] if present and a valid string; otherwise raise HubmapInputException
    """
    if st in data and data[st] is not None:
        return data[st]
    else:
        raise HubmapInputException(st)


"""
Parameters for this request (all required)

Key        Method    Type    Description
provider    post    string    Providing site, presumably a known TMC
sample_id   post    string    Unique ID string specifying this dataset
process     post    string    string denoting a unique known processing workflow to be applied to this data

Parameters included in the response:
Key        Type    Description
ingest_id  string  Unique ID string to be used in references to this request

"""
@csrf.exempt
@api_bp.route('/request_ingest', methods=['POST'])
def request_ingest():
    # decode input
    data = request.get_json(force=True)
    
    # Test and extract required parameters
    try:
        provider = _get_required_string(data, 'provider')
        sample_id = _get_required_string(data, 'sample_id')
        process = _get_required_string(data, 'process')
    except Exception as e:
        return HubmapApiResponse.bad_request('Must specify {} to request data be ingested'.format(str(e)))

    try:
        session = settings.Session()

#         dagbag = DagBag('dags')
# 
#         if dag_id not in dagbag.dags:
#             return HubmapApiResponse.bad_request("Dag id {} not found".format(dag_id))
# 
#         dag = dagbag.get_dag(dag_id)
# 
#         # ensure run data has all required attributes and that everything is valid, returns transformed data
#         runs = utils_date_range(start_date=start_date, end_date=end_date, delta=dag._schedule_interval)
# 
#         if len(runs) > limit and partial is False:
#             error = '{} dag runs would be created, which exceeds the limit of {}.' \
#                     ' Reduce start/end date to reduce the dag run count'
#             return HubmapApiResponse.bad_request(error.format(len(runs), limit))
# 
#         payloads = []
#         for exec_date in runs:
#             run_id = '{}_{}'.format(prefix, exec_date.isoformat())
# 
#             if find_dag_runs(session, dag_id, run_id, exec_date):
#                 continue
# 
#             payloads.append({
#                 'run_id': run_id,
#                 'execution_date': exec_date,
#                 'conf': conf
#             })
# 
#         results = []
#         for index, run in enumerate(payloads):
#             if len(results) >= limit:
#                 break
# 
#             dag.create_dagrun(
#                 run_id=run['run_id'],
#                 execution_date=run['execution_date'],
#                 state=State.RUNNING,
#                 conf=conf,
#                 external_trigger=True
#             )
#             results.append(run['run_id'])

        session.close()
    except HubmapApiInputException as e:
        return HubmapApiResponse.bad_request(str(e))
    except ValueError as e:
        return HubmapApiResponse.server_error(str(e))
    except AirflowException as e:
        return HubmapApiResponse.server_error(str(e))
    except Exception as e:
        return HubmapApiResponse.server_error(str(e))
    

    return HubmapApiResponse.success({'ingest_id': 'this_is_some_unique_string'})
