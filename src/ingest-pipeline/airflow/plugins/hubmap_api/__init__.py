from airflow.plugins_manager import AirflowPlugin

from hubmap_api.manager import aav1 as hubmap_api_admin_v1
from hubmap_api.manager import aav2 as hubmap_api_admin_v2
from hubmap_api.manager import aav3 as hubmap_api_admin_v3
from hubmap_api.manager import aav4 as hubmap_api_admin_v4
from hubmap_api.manager import blueprint as hubmap_api_blueprint

class AirflowHuBMAPPlugin(AirflowPlugin):
    name = "hubmap_api"
    operators = []
    sensors = []
    hooks = []
    executors = []
    macros = []
    admin_views = [hubmap_api_admin_v1, hubmap_api_admin_v2, hubmap_api_admin_v3,
                   hubmap_api_admin_v4]
    flask_blueprints = [hubmap_api_blueprint]
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []
    global_operator_extra_links = []
