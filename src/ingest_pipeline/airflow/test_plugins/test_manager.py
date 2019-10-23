import logging
from io import StringIO

# This is the class you derive to create a plugin
from airflow.plugins_manager import AirflowPlugin

from flask import Blueprint, current_app, send_from_directory, abort, escape
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

# Will show up under airflow.hooks.test_plugin.PluginHook
class PluginHook(BaseHook):
    pass

# Will show up under airflow.operators.test_plugin.PluginOperator
class PluginOperator(BaseOperator):
    pass

# Will show up under airflow.sensors.test_plugin.PluginSensorOperator
class PluginSensorOperator(BaseSensorOperator):
    pass

# Will show up under airflow.executors.test_plugin.PluginExecutor
class PluginExecutor(BaseExecutor):
    pass

# Will show up under airflow.macros.test_plugin.plugin_macro
# and in templates through {{ macros.test_plugin.plugin_macro }}
def plugin_macro():
    pass

def map_to_string(map):
    sio = StringIO()
    for elt in map.iter_rules():
        sio.write('{}<br>\n'.format(str(elt)))
    return escape(sio.getvalue())

# Creating a flask admin BaseView
class TestView(BaseView):
    @expose('/')
    def test(self):
        LOGGER.info('In TestView.test()')
        # in this example, put your test_plugin/test.html template at airflow/plugins/templates/test_plugin/test.html
        return self.render("test_plugin/test.html", content=map_to_string(current_app.url_map))
v = TestView(category="Test Plugin", name="Test View")

# Creating a flask blueprint to integrate the templates and static folder
bp = Blueprint(
    "test2_plugin", __name__,
    template_folder='templates', # registers airflow/plugins/templates as a Jinja template folder
    static_folder='static',
#    static_url_path='/static/test2_plugin'
)
@bp.route('/', defaults={'page':'index'})
@bp.route('/<page>')
def show(page):
    try:
        LOGGER.info('trying static page %s', page)
        return send_from_directory('/usr/local/airflow/plugins/static/test2_plugin/', page)
    except Exception as e:
        LOGGER.info('static page %s not found: %s', page, e)
        abort(404)

ml = MenuLink(
    category='Test Plugin',
    name='Test Menu Link',
    url='https://airflow.apache.org/')

# Creating a flask appbuilder BaseView
class TestAppBuilderBaseView(AppBuilderBaseView):
    @expose("/")
    def test(self):
        LOGGER.info('In TestAppBuilderBaseView.test')
        try:
            return self.render("test_plugin/test.html",
                               content='This content comes from TestAppBuilderBaseView')
        except Exception as e:
            LOGGER.info('TestAppBuilderBaseView failed: {}'.format(e))
            Abort(404)

v_appbuilder_view = TestAppBuilderBaseView()
v_appbuilder_package = {"name": "Test View 2",
                        "category": "Test2Plugin",
                        "view": v_appbuilder_view}

# Creating a flask appbuilder Menu Item
appbuilder_mitem = {"name": "Google",
                    "category": "Search",
                    "category_icon": "fa-th",
                    "href": "https://www.google.com"}

# # A global operator extra link that redirect you to
# # task logs stored in S3
# class S3LogLink(BaseOperatorLink):
#     name = 'S3'

#     def get_link(self, operator, dttm):
#         return 'https://s3.amazonaws.com/airflow-logs/{dag_id}/{task_id}/{execution_date}'.format(
#             dag_id=operator.dag_id,
#             task_id=operator.task_id,
#             execution_date=dttm,
#         )

# A global operator extra link that redirect you to
# www.psc.edu
class PSCLink(BaseOperatorLink):
    name = 'PSC'

    def get_link(self, operator, dttm):
        return 'https://www.psc.edu/'


# Defining the plugin class
class AirflowTestPlugin(AirflowPlugin):
    name = "test_plugin"
    operators = [PluginOperator]
    sensors = [PluginSensorOperator]
    hooks = [PluginHook]
    executors = [PluginExecutor]
    macros = [plugin_macro]
    admin_views = [v]
    flask_blueprints = [bp]
    menu_links = [ml]
    appbuilder_views = [v_appbuilder_package]
    appbuilder_menu_items = [appbuilder_mitem]
    global_operator_extra_links = [PSCLink(),] # [S3LogLink(),]
