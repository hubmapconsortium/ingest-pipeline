import os
import logging
from io import StringIO

# This is the class you derive to create a plugin
from airflow.plugins_manager import AirflowPlugin

from werkzeug.exceptions import HTTPException, NotFound 

from flask import Blueprint, current_app, send_from_directory, abort, escape, render_template
from flask_admin import BaseView, expose
from flask_appbuilder import AppBuilder
from flask_appbuilder import BaseView as AppBuilderBaseView
from flask_admin.base import MenuLink

# Importing base classes that we need to derive
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.models.baseoperator import BaseOperatorLink
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.executors.base_executor import BaseExecutor

from jinja2 import TemplateNotFound

LOGGER = logging.getLogger(__name__)

def map_to_list(map):
    lst = []
    for elt in map.iter_rules():
        lst.append(elt)
    return lst


class APIAdminView1(BaseView):
    @expose('/')
    def api_admin_view1(self):
        LOGGER.info('In APIAdminView1.api_admin_view1')
        return render_template('generic.html',
                               title='Known Routes', 
                               content_lst=map_to_list(current_app.url_map))
aav1 = APIAdminView1(category='HuBMAP API', name="Known Routes")


# Create a Flask blueprint to hold the HuBMAP API
bp = Blueprint(
    "hubmap_api", __name__,
    url_prefix='/api/hubmap',
    template_folder='templates',
    static_folder='static',
)

@bp.route('/static/', defaults={'page':'index.html'})
@bp.route('/static/<page>')
def show_static(page):
    try:
        static_dir = os.path.join(os.path.dirname(__file__), 'static')
        return send_from_directory(static_dir, page)
    except NotFound as e:
        LOGGER.info('static page {0} not found: {1}'.format(page, repr(e)))
        abort(404)

@bp.route('/templates/', defaults={'page': 'index.html'})
@bp.route('/templates/<page>')
def show_template(page, title=None, content=None, content_lst=None):
    try:
        return render_template(page,
                               title=title,
                               content=content,
                               content_lst=map_to_list(current_app.url_map))
    except TemplateNotFound as e:
        LOGGER.info('template page {0} not found: {1}'.format(page, e))
        abort(404)


# Defining the plugin class
class AirflowHuBMAPPlugin(AirflowPlugin):
    name = "HuBMAP_plugin"
    operators = []
    sensors = []
    hooks = []
    executors = []
    macros = []
    admin_views = [aav1]
    flask_blueprints = [bp]
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []
    global_operator_extra_links = []
