import os
import logging
import json

from werkzeug.exceptions import NotFound

from flask import Blueprint, current_app, send_from_directory, abort, render_template, request, redirect, url_for
from flask import session as f_session

from airflow.configuration import conf as airflow_conf
from airflow import models, settings
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from airflow.utils.state import State
from airflow.utils import timezone

from jinja2 import TemplateNotFound

LOGGER = logging.getLogger(__name__)

def map_to_list(map):
    lst = []
    for elt in map.iter_rules():
        lst.append(elt)
    return lst


class APIAdminView1(AppBuilderBaseView):
    default_view = "api_admin_view1"

    @expose('/')
    def api_admin_view1(self):
        LOGGER.info('In APIAdminView1.api_admin_view1')
        return show_template('generic.html',
                             title='Known Routes',
                             content_lst=map_to_list(current_app.url_map))
aav1 = APIAdminView1()
aav1_package = {'category': 'SenNet API',
                'name': 'Known Routes',
                'view': aav1}


class APIAdminView2(AppBuilderBaseView):
    default_view = "api_admin_view2"

    @expose('/')
    def api_admin_view2(self):
        LOGGER.info('In APIAdminView1.api_admin_view2')
        return show_template('generic.html',
                             title='Flask Config',
                             content_lst=["{0} = {1}".format(k, v) for k, v in current_app.config.items()])
aav2 = APIAdminView2()
aav2_package = {'category': 'SenNet API',
                'name': 'Flask Config',
                'view': aav2}


class APIAdminView3(AppBuilderBaseView):
    default_view = "api_admin_view3"

    @expose('/')
    def api_admin_view3(self):
        LOGGER.info('In APIAdminView1.api_admin_view3')
        return show_template('show_config.html',
                             title='Airflow Config',
                             dict_of_dicts=airflow_conf.as_dict())
aav3 = APIAdminView3()
aav3_package = {'category': 'SenNet API',
                'name': 'Airflow Config',
                'view': aav3}


class APIAdminView4(AppBuilderBaseView):
    default_view = "api_admin_view4"

    @expose('/')
    def api_admin_view4(self):
        LOGGER.info('In APIAdminView1.api_admin_view4')
        eltL = ["{0} = {1}".format(k, v) for k, v in os.environ.items()]
        eltL.sort()
        return show_template('generic.html',
                             title='Environment Variables',
                             content_lst=eltL)
aav4 = APIAdminView4()
aav4_package = {'category': 'SenNet API',
                'name': 'Environment Variables',
                'view': aav4}


class APIAdminView5(AppBuilderBaseView):
    default_view = "api_admin_view5"

    @expose('/', methods=['GET', 'POST'])
    def api_admin_view5(self):
        LOGGER.info('Triggering Globus Transfer DAG')
        dag_id = 'globus_transfer'

        dagbag = models.DagBag(settings.DAGS_FOLDER)

        execution_date = timezone.utcnow()
        run_id = "manual__{0}".format(execution_date.isoformat())

        dag = dagbag.get_dag(dag_id)
        if request.method == 'GET':
            return show_template(
                'trigger.html', dag_id=dag_id, origin='/home', conf=''
            )

        request_conf = request.values.get('conf')

        run_conf = {'tokens': f_session['tokens'], 'conf': json.loads(request_conf)}

        dag.create_dagrun(
            run_id=run_id,
            execution_date=execution_date,
            state=State.RUNNING,
            conf=run_conf,
            external_trigger=True
        )
        return redirect(url_for('admin.index'))

aav5 = APIAdminView5()
aav5_package = {'category': 'SenNet API',
                'name': 'Trigger Test Globus Transfer',
                'view': aav5}


class APIAdminView6(AppBuilderBaseView):
    default_view = "api_admin_view6"

    @expose('/', methods=['GET'])
    def api_admin_view6(self):
        LOGGER.info('Triggering Globus Logout routine')
        return redirect(self.appbuilder.get_url_for_logout)


aav6 = APIAdminView6()
aav6_package = {'category': 'SenNet API',
                'name': 'Globus Logout',
                'view': aav6}


# Create a Flask blueprint to hold the SenNet API
blueprint = Blueprint(
    "hubmap_api", __name__,
    url_prefix='/api/sennet',
    template_folder='templates',
    static_folder='static',
)

@blueprint.route('/static/', defaults={'page':'index.html'})
@blueprint.route('/static/<page>')
def show_static(page):
    try:
        static_dir = os.path.join(os.path.dirname(__file__), 'static')
        return send_from_directory(static_dir, page)
    except NotFound as e:
        LOGGER.info('static page {0} not found: {1}'.format(page, repr(e)))
        abort(404)

@blueprint.route('/templates/', defaults={'page': 'index.html'})
@blueprint.route('/templates/<page>')
def show_template(page, **kwargs):
    try:
        return render_template(page, **kwargs)
    except TemplateNotFound as e:
        LOGGER.info('template page {0} not found: {1}'.format(page, e))
        abort(404)

