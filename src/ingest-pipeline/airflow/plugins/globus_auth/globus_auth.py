import flask_login

import globus_sdk

# Need to expose these downstream
# flake8: noqa: F401
from flask_login import current_user, logout_user, login_required, login_user

from flask import url_for, redirect, request

from flask_oauthlib.client import OAuth

from airflow import models, configuration
from airflow.utils.db import provide_session
from airflow.utils.log.logging_mixin import LoggingMixin

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

    def init_app(self, flask_app):
        self.flask_app = flask_app

        self.login_manager.init_app(self.flask_app)

        self.globus_oauth = globus_sdk.ConfidentialAppAuthClient(
        get_config_param('APP_CLIENT_ID'), get_config_param('APP_CLIENT_SECRET'))

        self.login_manager.user_loader(self.load_user)

        self.flask_app.add_url_rule('/login',
                                    'login',
                                    self.login)

    @provide_session
    def login(self, request=None, session=None):
        log.debug('Redirecting user to Globus login')

        redirect_url = url_for('login', _external=True)

        self.globus_oauth.oauth2_start_flow(redirect_url)

        try:
            if 'code' not in request.args:
                auth_uri = self.globus_oauth.oauth2_get_authorize_url(additional_params={
                    "scope": "openid profile email urn:globus:auth:scope:transfer.api.globus.org:all urn:globus:auth:scope:auth.globus.org:view_identities urn:globus:auth:scope:nexus.api.globus.org:groups"})
                return redirect(auth_uri)
            else:
                code = request.args.get('code')
                tokens = self.globus_oauth.oauth2_exchange_code_for_tokens(code)

                globus_token = tokens.by_resource_server['auth.globus.org']['access_token']

                username, email = self.get_globus_user_profile_info(globus_token)

                # store the resulting tokens in the session
                session.update(
                    tokens=tokens.by_resource_server,
                    is_authenticated=True
                )
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

                next_url = request.args.get('state') or url_for('admin.index')
                return redirect(next_url)
        except AuthenticationError:
            return redirect(url_for('airflow.noaccess'))


    def get_globus_user_profile_info(self, globus_token):
        resp = self.globus_oauth.oauth2_userinfo()

        if not resp or resp.status != 200:
            raise AuthenticationError(
                'Failed to fetch user profile, status ({0})'.format(
                    resp.status if resp else 'None'))

        return resp['name'], resp['email']

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