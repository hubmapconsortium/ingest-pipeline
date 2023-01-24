# -*- coding: utf-8 -*-

import os

import globus_sdk
from airflow.configuration import conf
from airflow import configuration
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.www.security import AirflowSecurityManager
from flask import flash, g, url_for, request
from flask_appbuilder import expose
from flask_appbuilder._compat import as_unicode
from flask_appbuilder.const import AUTH_OAUTH
from flask_appbuilder.security.views import AuthOAuthView
from flask_login import login_user, logout_user
from hubmap_commons.hm_auth import AuthHelper
from werkzeug.utils import redirect
from flask import session as f_session

log = LoggingMixin().log


# ToDo: try to use the standard way of getting info from OAUTH_PROVIDERS
def get_config_param(param):
    return str(configuration.conf.get('globus', param))


class CustomOAuthView(AuthOAuthView):

    def __init__(self):
        super().__init__()
        self.group_uuids = []
        client_id = get_config_param('APP_CLIENT_ID')
        client_secret = get_config_param('APP_CLIENT_SECRET')

        self.globus_oauth = globus_sdk.ConfidentialAppAuthClient(get_config_param('APP_CLIENT_ID'),
                                                                 get_config_param('APP_CLIENT_SECRET'))

        if not AuthHelper.isInitialized():
            self.authHelper = AuthHelper.create(clientId=client_id, clientSecret=client_secret)
        else:
            self.authHelper = AuthHelper.instance()

        groups_with_permission_by_name = [group.strip().lower() for group in
                                          get_config_param('hubmap_groups').split(',')]

        groups_by_name = AuthHelper.getHuBMAPGroupInfo()

        for group_with_permission in groups_with_permission_by_name:
            if group_with_permission in groups_by_name and groups_by_name[group_with_permission][
                'uuid'] not in self.group_uuids:
                self.group_uuids.append(groups_by_name[group_with_permission]['uuid'])
            else:
                log.error('Invalid group name provided in configuration: ' + group_with_permission)

    @expose("/login/", methods=["GET", "POST"])
    def login(self):
        if g.user is not None and g.user.is_authenticated:
            return redirect(self.appbuilder.get_url_for_index)
        redirect_url = url_for('.login', _external=True, _scheme=get_config_param('scheme')).rstrip('/')

        self.globus_oauth.oauth2_start_flow(redirect_url)

        if 'code' not in request.args:
            auth_uri = self.globus_oauth.oauth2_get_authorize_url(query_params  ={
                "scope": "openid profile email urn:globus:auth:scope:transfer.api.globus.org:all "
                         "urn:globus:auth:scope:auth.globus.org:view_identities "
                         "urn:globus:auth:scope:groups.api.globus.org:all"})
            return redirect(auth_uri)
        else:
            code = request.args.get('code')
            try:
                tokens = self.globus_oauth.oauth2_exchange_code_for_tokens(code)
            except Exception as e:
                log.info(f'Exception on oauth2, probably invalid token {e}')
                return redirect(self.appbuilder.get_url_for_login)
            f_session['tokens'] = tokens.by_resource_server

            user_info = self.get_globus_user_profile_info(
                tokens.by_resource_server['groups.api.globus.org']['access_token'])
            user_info.pop('username', None)
            username = user_info['name']
            email = user_info['email']
            group_ids = user_info['hmgroupids']

            if not (group_ids and list(set(group_ids) & set(self.group_uuids))):
                raise Exception('User does not have correct group assignments')

            user = self.appbuilder.sm.auth_user_oauth(user_info)
            if not user:
                user = self.appbuilder.sm.add_user(username=email,
                                                   first_name=username.split()[0],
                                                   last_name=username.split()[1],
                                                   email=email,
                                                   role=self.appbuilder.sm.find_role('Admin'))

            if user is None:
                flash(as_unicode(self.invalid_login_message), "warning")
                return redirect(self.appbuilder.get_url_for_login)
            else:
                login_user(user)
                self.appbuilder.sm.update_user_auth_stat(user)
                return redirect(self.appbuilder.get_url_for_index)

    @expose("/logout/", methods=["GET", "POST"])
    def logout(self, seesion=None):
        # Revoke the tokens with Globus Auth
        if 'tokens' in f_session:
            for token in (token_info['access_token']
                          for token_info in f_session['tokens'].values()):
                self.globus_oauth.oauth2_revoke_token(token)

        # Destroy the session state
        f_session.clear()

        # the return redirection location to give to Globus Auth
        redirect_url = url_for('Airflow.index', _external=True, _scheme=get_config_param('scheme'))

        # build the logout URI with query params
        # there is no tool to help build this (yet!)
        globus_logout_url = (
                'https://auth.globus.org/v2/web/logout' +
                '?client={}'.format(
                    get_config_param('APP_CLIENT_ID')) +
                '&redirect_uri={}'.format(redirect_url) +
                '&redirect_name=Airflow Home')

        # Redirect the user to the Globus Auth logout page
        logout_user()
        return redirect(globus_logout_url)

    def get_globus_user_profile_info(self, token):
        return self.authHelper.getUserInfo(token, True)


class OIDCSecurityManager(AirflowSecurityManager):
    """
    Custom security manager class that allows using the OpenID Connection authentication method.
    """

    def __init__(self, appbuilder):
        super(OIDCSecurityManager, self).__init__(appbuilder)
        self.authoauthview = CustomOAuthView


AUTH_TYPE = AUTH_OAUTH
AUTH_ROLE_ADMIN = 'Admin'
AUTH_ROLE_PUBLIC = 'Public'

SECURITY_MANAGER_CLASS = OIDCSecurityManager

basedir = os.path.abspath(os.path.dirname(__file__))

# The SQLAlchemy connection string.
SQLALCHEMY_DATABASE_URI = conf.get('core', 'SQL_ALCHEMY_CONN')

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = True

OAUTH_PROVIDERS = [{
    'name': 'Globus',
    'icon': 'fa-Twitter',
    'remote_app': {
        'base_url': 'https://auth.globus.org/',
        'consumer_key': "dummy",
        'consumer_secret': "dummy",
    }
}]