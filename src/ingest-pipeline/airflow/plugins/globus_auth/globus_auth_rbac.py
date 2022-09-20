# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Default configuration for the Airflow webserver"""
import os
import re

import globus_sdk
from airflow.configuration import conf
from airflow import configuration, LoggingMixin, models
from airflow.www_rbac.security import AirflowSecurityManager
from flask import flash, g, url_for, request
from flask_appbuilder import expose
from flask_appbuilder._compat import as_unicode
from flask_appbuilder.const import AUTH_OAUTH
from flask_appbuilder.security.views import AuthOAuthView
from flask_login import login_user
from hubmap_commons.hm_auth import AuthHelper
from werkzeug.utils import redirect
from flask import session as f_session

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
        redirect_url = url_for('.login', _external=True, _scheme=get_config_param('scheme'))

        self.globus_oauth.oauth2_start_flow("https://hivevm202.psc.edu:5555/login")

        if 'code' not in request.args:
            auth_uri = self.globus_oauth.oauth2_get_authorize_url(additional_params={
                "scope": "openid profile email urn:globus:auth:scope:transfer.api.globus.org:all "
                         "urn:globus:auth:scope:auth.globus.org:view_identities "
                         "urn:globus:auth:scope:groups.api.globus.org:all"})
            return redirect(auth_uri)
        else:
            code = request.args.get('code')
            tokens = self.globus_oauth.oauth2_exchange_code_for_tokens(code)
            f_session['tokens'] = tokens.by_resource_server

            user_info = self.get_globus_user_profile_info(
                tokens.by_resource_server['groups.api.globus.org']['access_token'])
            username = user_info['name']
            email = user_info['email']
            group_ids = user_info['hmgroupids']

            if not (group_ids and list(set(group_ids) & set(self.group_uuids))):
                raise Exception('User does not have correct group assignments')

            user = self.appbuilder.sm.auth_user_oauth(user_info)
            if not user:
                user = models.User(
                    username=username,
                    email=email,
                    is_superuser=False)

            if user is None:
                flash(as_unicode(self.invalid_login_message), "warning")
                return redirect(self.appbuilder.get_url_for_login)
            else:
                login_user(GlobusUser(user))
                return redirect(self.appbuilder.get_url_for_index)

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
SECURITY_MANAGER_CLASS = OIDCSecurityManager

basedir = os.path.abspath(os.path.dirname(__file__))

# The SQLAlchemy connection string.
SQLALCHEMY_DATABASE_URI = conf.get('core', 'SQL_ALCHEMY_CONN')

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = True

OAUTH_PROVIDERS = [{
    'name': 'Globus',
    'token_key': 'access_token',
    'icon': 'fa-Twitter',
    'remote_app': {
        'base_url': 'https://auth.globus.org/',
        'request_token_params': {
            'scope': 'email profile'
        },
        'access_token_url': 'https://auth.globus.org/p',
        'authorize_url': 'https://auth.globus.org/p/login',
        'request_token_url': None,
        'consumer_key': "21f293b0-5fa5-4ee1-9e0e-3cf88bd70114",
        'consumer_secret': "gimzYEgm/jMtPmNJ0qoV11gdicAK8dgu+yigj2m3MTE=",
    }
}]
