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

import globus_sdk
from airflow.configuration import conf
from airflow import configuration, LoggingMixin, models
from airflow.www_rbac.security import AirflowSecurityManager
from flask_appbuilder import expose
from flask_appbuilder.const import AUTH_REMOTE_USER
from flask_appbuilder.security.forms import LoginForm_db
from flask_appbuilder.security.views import AuthRemoteUserView
from flask import redirect, g, url_for, request
from flask import session as f_session
from flask_babel import lazy_gettext
from flask_login import logout_user, login_user

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


class CustomAuthRemoteUserView(AuthRemoteUserView):
    login_template = "appbuilder/general/security/login_db.html"
    route_base = ""
    invalid_login_message = lazy_gettext("Invalid login. Please try again.")
    title = lazy_gettext("Sign In")
    globus_oauth = globus_sdk.ConfidentialAppAuthClient(get_config_param('APP_CLIENT_ID'),
                                                        get_config_param('APP_CLIENT_SECRET'))
    group_uuids = []
    authHelper = None

    @expose("/login/", methods=["GET", "POST"])
    def login(self):
        log.debug('Redirecting user to Globus login')

        redirect_url = url_for('Airflow.index', _external=True, _scheme=get_config_param('scheme'))

        self.globus_oauth.oauth2_start_flow(redirect_url)

        try:
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

                if not g.user:
                    user = models.User(
                        username=username,
                        email=email,
                        is_superuser=False)
                else:
                    user = g.user
                login_user(GlobusUser(user))

                next_url = url_for('admin.index')
                return redirect(next_url)
        except Exception as e:
            log.error(e)
            return redirect(url_for('airflow.noaccess'))

    @expose("/logout/")
    def logout(self):
        logout_user()
        return redirect(self.appbuilder.get_url_for_index)

    def get_globus_user_profile_info(self, token):
        return self.authHelper.getUserInfo(token, True)


class OIDCSecurityManager(AirflowSecurityManager):
    """
    Custom security manager class that allows using the OpenID Connection authentication method.
    """

    def __init__(self, appbuilder):
        super(OIDCSecurityManager, self).__init__(appbuilder)
        self.authremoteuserview = CustomAuthRemoteUserView


AUTH_TYPE = AUTH_REMOTE_USER
SECURITY_MANAGER_CLASS = OIDCSecurityManager
basedir = os.path.abspath(os.path.dirname(__file__))

# The SQLAlchemy connection string.
SQLALCHEMY_DATABASE_URI = conf.get('core', 'SQL_ALCHEMY_CONN')

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = True
