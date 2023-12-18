from pprint import pprint
from typing import Tuple

from airflow.providers.http.hooks.http import HttpHook
from airflow.configuration import conf as airflow_conf


def check_link_published_drvs(uuid: str) -> Tuple[bool, str]:
    needs_previous_version = False
    published_uuid = ''
    endpoint = f'/children/{uuid}'
    auth_tok = ''.join(e for e in airflow_conf.as_dict()['connections']['APP_CLIENT_SECRET'] if e.isalnum())
    headers = {
        'authorization': 'Bearer ' + auth_tok,
        'content-type': 'application/json',
        'X-Hubmap-Application': 'ingest-pipeline'}
    extra_options = {}

    http_hook = HttpHook('GET', http_conn_id='entity_api_connection')

    response = http_hook.run(endpoint,
                             headers=headers,
                             extra_options=extra_options)
    print('response: ')
    pprint(response.json())
    for data in response.json():
        if data.get('entity_type') in ('Dataset', 'Publication') and data.get('status') == 'Published':
            needs_previous_version = True
            published_uuid = data.get('uuid')
    return needs_previous_version, published_uuid
