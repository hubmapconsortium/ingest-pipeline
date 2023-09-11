from pprint import pprint
from typing import Tuple

from airflow.providers.http.hooks.http import HttpHook

from utils import get_auth_tok


def check_link_published_drvs(uuid: str, **kwargs) -> Tuple[bool, str]:
    needs_previous_version = False
    published_uuid = ''
    endpoint = f'/children/{uuid}'
    headers = {
        'authorization': 'Bearer ' + get_auth_tok(**kwargs),
        'content-type': 'application/json',
        'X-Hubmap-Application': 'ingest-pipeline'}
    extra_options = {}

    http_hook = HttpHook('GET', http_conn_id='entity_api_connection')

    response = http_hook.run(endpoint,
                             headers,
                             extra_options)
    print('response: ')
    pprint(response.json())
    for data in response.json():
        if data.get('entity_type') in ('Dataset', 'Publication') and data.get('status') == 'Published':
            needs_previous_version = True
            published_uuid = data.get('uuid')
    return needs_previous_version, published_uuid
