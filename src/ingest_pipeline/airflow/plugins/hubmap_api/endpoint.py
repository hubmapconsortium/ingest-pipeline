"""
Based heavily on https://github.com/airflow-plugins/airflow_api_plugin
"""

import json
import os
import logging
import configparser
from datetime import datetime
import pytz

from werkzeug.exceptions import HTTPException, NotFound 

from flask import Blueprint, current_app, send_from_directory, abort, escape, request, Response
from sqlalchemy import or_
from airflow import settings
from airflow.exceptions import AirflowException, AirflowConfigException
from airflow.www.app import csrf
from airflow.models import DagBag, DagRun, Variable
from airflow.utils import timezone
from airflow.utils.dates import date_range as utils_date_range
from airflow.utils.state import State
from airflow.api.common.experimental import trigger_dag

from hubmap_api.manager import blueprint as api_bp
from hubmap_api.manager import show_template

LOGGER = logging.getLogger(__name__)
CONFIG = None  # specific to this API

class HubmapApiInputException(Exception):
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


@api_bp.route('/test')
def api_test():
    return HubmapApiResponse.success({'api_is_alive': True})
 

@api_bp.before_request
def verify_authentication():
    authorization = request.headers.get('authorization')
    try:
        api_auth_key = settings.conf.get('HUBMAP_API_PLUGIN', 'HUBMAP_API_AUTH')
    except AirflowConfigException:
        return
 
    if authorization != api_auth_key:
        return HubmapApiResponse.unauthorized("You are not authorized to use this resource")

@api_bp.before_request
def verify_conf():
    global CONFIG
    if CONFIG is None:
        conf_path = os.path.join(os.path.dirname(__file__), 'conf', 'hubmap_api.cnf')
        LOGGER.info('config path: {}'.format(conf_path))
        conf = configparser.ConfigParser()
        conf.read(conf_path)
        dd_conf = {}
        for elt in conf.sections():
            dd_conf[elt] = {}
            for elt2 in conf[elt]:
                dd_conf[elt][elt2] = conf[elt][elt2]
        
        # Check required sections
        for sec in ['ingest_map', 'core']:
            if sec not in dd_conf:
                dd_conf[sec] = {}

        CONFIG = dd_conf
        LOGGER.info('Imported config = {}'.format(json.dumps(CONFIG)))

    return None
        
 
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
run_id     string  The identifier by which the ingest run is known to Airflow

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

    process = process.lower()  # necessary because config parser has made the corresponding string lower case

    if process not in CONFIG['ingest_map']:
        return HubmapApiResponse.bad_request('{} is not a known ingestion process'.format(process))

    dag_id = CONFIG['ingest_map'][process]
    
    try:
        session = settings.Session()

        dagbag = DagBag('dags')
 
        if dag_id not in dagbag.dags:
            return HubmapApiResponse.bad_request("Dag id {} not found".format(dag_id))
 
        dag = dagbag.get_dag(dag_id)

        # Produce one and only one run
        tz = pytz.timezone(CONFIG['core']['timezone'])
        execution_date = datetime.now(tz)
        LOGGER.info('execution_date: {}'.format(execution_date))

        conf = {'provider': provider,
                'sample_id': sample_id,
                'process': process,
                'dag_id': dag_id
                }

        run_id = '{}_{}_{}'.format(sample_id, process, execution_date.isoformat())

        if find_dag_runs(session, dag_id, run_id, execution_date):
            # The run already happened??
            return HubmapAPIResponse.server_error('The request happened twice?')

        payload = {
            'run_id': run_id,
            'execution_date': execution_date,
            'conf': conf
            }
 
        LOGGER.info('point 1')
        try:
            dr = trigger_dag.trigger_dag(dag_id, payload['run_id'], payload['conf'], execution_date=execution_date)
        except AirflowException as err:
            LOGGER.error(err)
            return HubmapApiResponse.bad_request("Attempt to trigger run produced an error: {}".format(err))
        LOGGER.info('dr follows: {}'.format(dr))

#             dag.create_dagrun(
#                 run_id=run['run_id'],
#                 execution_date=run['execution_date'],
#                 state=State.RUNNING,
#                 conf=conf,
#                 external_trigger=True
#             )
#            results.append(run['run_id'])

        session.close()
    except HubmapApiInputException as e:
        return HubmapApiResponse.bad_request(str(e))
    except ValueError as e:
        return HubmapApiResponse.server_error(str(e))
    except AirflowException as e:
        return HubmapApiResponse.server_error(str(e))
    except Exception as e:
        return HubmapApiResponse.server_error(str(e))

    return HubmapApiResponse.success({'ingest_id': 'this_is_some_unique_string',
                                      'run_id': payload['run_id']})
