import flask_login

import globus_sdk

# Need to expose these downstream
# flake8: noqa: F401
from flask_login import current_user, logout_user, login_required, login_user

from flask import url_for, redirect, request
from flask import session as f_session

from flask_oauthlib.client import OAuth

from airflow import models, configuration
from airflow.utils.db import provide_session
from airflow.utils.log.logging_mixin import LoggingMixin

from hubmap_commons.hm_auth import AuthHelper

log = LoggingMixin().log


def get_config_param(param):
    return str(configuration.conf.get('globus', param))


class GlobusUser(models.User):

    def __init__(self, user):
        self.user = user

    @property
    def is_active(self):
        """Required by flask_login"""
        return True

    @property
    def is_authenticated(self):
        """Required by flask_login"""
        return True

    @property
    def is_anonymous(self):
        """Required by flask_login"""
        return False

    def get_id(self):
        """Returns the current user id as required by flask_login"""
        return self.user.get_id()

    def data_profiling(self):
        """Provides access to data profiling tools"""
        return True

    def is_superuser(self):
        """Access all the things"""
        return True


class AuthenticationError(Exception):
    pass


class GlobusAuthBackend(object):

    def __init__(self):
        # self.globus_host = get_config_param('host')
        self.login_manager = flask_login.LoginManager()
        self.login_manager.login_view = 'airflow.login'
        self.flask_app = None
        self.globus_oauth = None
        self.api_rev = None
        self.authHelper = None
        self.group_uuids = []

    def init_app(self, flask_app):
        client_id = get_config_param('APP_CLIENT_ID')
        client_secret = get_config_param('APP_CLIENT_SECRET')

        self.flask_app = flask_app

        self.login_manager.init_app(self.flask_app)

        self.globus_oauth = globus_sdk.ConfidentialAppAuthClient(
        get_config_param('APP_CLIENT_ID'), get_config_param('APP_CLIENT_SECRET'))

        self.login_manager.user_loader(self.load_user)

        self.flask_app.add_url_rule('/login',
                                    'login',
                                    self.login)

        if not AuthHelper.isInitialized():
            self.authHelper = AuthHelper.create(clientId=client_id, clientSecret=client_secret)
        else:
            self.authHelper = AuthHelper.instance()

        groups_with_permission_by_name = get_config_param('hubmap_groups').split(',')
        groups_by_name = AuthHelper.getHuBMAPGroupInfo()

        for group_with_permission in groups_with_permission_by_name:
            if group_with_permission in groups_by_name and groups_by_name[group_with_permission]['uuid'] not in self.group_uuids:
                self.group_uuids.append(groups_by_name[group_with_permission]['uuid'])
            else:
                log.error('Invalid group name provided in configuration: ' + group_with_permission)

    @provide_session
    def login(self, session=None):
        log.debug('Redirecting user to Globus login')

        redirect_url = url_for('login', _external=True, _scheme=get_config_param('scheme'))

        self.globus_oauth.oauth2_start_flow(redirect_url)

        try:
            if 'code' not in request.args:
                auth_uri = self.globus_oauth.oauth2_get_authorize_url(additional_params={
                    "scope": "openid profile email urn:globus:auth:scope:transfer.api.globus.org:all urn:globus:auth:scope:auth.globus.org:view_identities urn:globus:auth:scope:nexus.api.globus.org:groups"})
                return redirect(auth_uri)
            else:
                code = request.args.get('code')
                tokens = self.globus_oauth.oauth2_exchange_code_for_tokens(code)
                f_session['tokens'] = tokens.by_resource_server

                user_info = self.get_globus_user_profile_info(
                    tokens.by_resource_server['nexus.api.globus.org']['access_token'])
                username = user_info['name']
                email = user_info['email']
                group_ids = user_info['hmgroupids']

                if group_ids is None or list(set(group_ids) & set(self.group_uuids)) is None:
                    raise Exception('User does not have correct group assignments')

                user = session.query(models.User).filter(
                    models.User.username == username).first()

                if not user:
                    user = models.User(
                        username=username,
                        email=email,
                        is_superuser=False)

                session.merge(user)
                session.commit()
                login_user(GlobusUser(user))
                session.commit()

                next_url = url_for('admin.index')
                return redirect(next_url)
        except Exception as e:
            log.error(e)
            return redirect(url_for('airflow.noaccess'))

    def get_globus_user_profile_info(self, token):
        return self.authHelper.getUserInfo(token, True)

    @provide_session
    def load_user(self, userid, session=None):
        if not userid or userid == 'None':
            return None

        user = session.query(models.User).filter(
            models.User.id == int(userid)).first()
        return GlobusUser(user)

login_manager = GlobusAuthBackend()

def login(self, request):
    return login_manager.login(request)