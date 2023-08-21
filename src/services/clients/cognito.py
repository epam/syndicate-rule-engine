from typing import Optional, Union

import boto3

from connections.auth_extension.base_auth_client import BaseAuthClient
from helpers import CustodianException, RESPONSE_INTERNAL_SERVER_ERROR
from helpers.log_helper import get_logger
from helpers.time_helper import utc_iso, utc_datetime
from services.environment_service import EnvironmentService
from helpers.system_customer import SYSTEM_CUSTOMER
from helpers.constants import CUSTOM_CUSTOMER_ATTR, CUSTOM_ROLE_ATTR, \
    CUSTOM_TENANTS_ATTR, CUSTOM_LATEST_LOGIN_ATTR

_LOG = get_logger(__name__)


PARAM_USER_POOLS = 'UserPools'


class CognitoClient(BaseAuthClient):
    def __init__(self, environment_service: EnvironmentService):
        self._environment = environment_service
        self._client = None
        self._user_pool_id, self._client_id = None, None

    @property
    def client(self):
        if not self._client:
            self._client = boto3.client('cognito-idp',
                                        self._environment.aws_region())
        return self._client

    @property
    def user_pool_name(self) -> str:
        return self._environment.get_user_pool_name()

    @property
    def user_pool_id(self) -> str:
        if not self._user_pool_id:
            _LOG.info('Retrieving user pool id')
            _id = self._environment.get_user_pool_id()
            if not _id:
                _LOG.warning('User pool id is not found in envs. '
                             'Scanning all the available pools to get the id')
                _id = self._pool_id_from_name(self.user_pool_name)
            if not _id:
                _message = 'Application Authentication Service is ' \
                           'not configured properly.'
                _LOG.error(f'User pool \'{self.user_pool_name}\' does '
                           f'not exists. {_message}')
                raise CustodianException(code=RESPONSE_INTERNAL_SERVER_ERROR,
                                         content=_message)
            self._user_pool_id = _id
        return self._user_pool_id

    @property
    def client_id(self) -> str:
        if not self._client_id:
            client = self.client.list_user_pool_clients(
                UserPoolId=self.user_pool_id, MaxResults=1)['UserPoolClients']
            if not client:
                _message = 'Application Authentication Service is not ' \
                           'configured properly: no client applications found'
                _LOG.error(_message)
                raise CustodianException(
                    code=RESPONSE_INTERNAL_SERVER_ERROR,
                    content=_message)
            self._client_id = client[0]['ClientId']
        return self._client_id

    def _pool_id_from_name(self, name: str) -> Optional[str]:
        """
        Since AWS Cognito can have two different pools with equal names,
        this method returns the first pool id which will be found.
        """
        pools = (p for p in self._list_user_pools() if p['Name'] == name)
        try:
            return next(pools)['Id']
        except StopIteration:
            return

    def _list_user_pools(self) -> list:
        list_user_pools = self.client.list_user_pools(MaxResults=10)
        pools = []
        while list_user_pools[PARAM_USER_POOLS]:
            pools.extend(list_user_pools[PARAM_USER_POOLS])
            next_token = list_user_pools.get('NextToken')
            if next_token:
                list_user_pools = self.client.list_user_pools(
                    MaxResults=10, NextToken=next_token)
            else:
                break
        return pools

    def _list_users(self, attributes_to_get=None):
        params = dict(UserPoolId=self.user_pool_id)
        if attributes_to_get:
            params['AttributesToGet'] = attributes_to_get
        return self.client.list_users(**params)

    def admin_initiate_auth(self, username, password):
        """
        Initiates the authentication flow. Returns AuthenticationResult if
        the caller does not need to pass another challenge. If the caller
        does need to pass another challenge before it gets tokens,
        ChallengeName, ChallengeParameters, and Session are returned.
        """
        auth_params = {
            'USERNAME': username,
            'PASSWORD': password
        }
        if self.is_user_exists(username):
            try:
                result = self.client.admin_initiate_auth(
                    UserPoolId=self.user_pool_id, ClientId=self.client_id,
                    AuthFlow='ADMIN_NO_SRP_AUTH', AuthParameters=auth_params)
                self.update_latest_login(username)
                return result
            except self.client.exceptions.NotAuthorizedException:
                return None

    def admin_delete_user(self, username: str):
        self.client.admin_delete_user(UserPoolId=self.user_pool_id,
                                      Username=username)

    def respond_to_auth_challenge(self, challenge_name):
        """
        Responds to an authentication challenge.
        """
        self.client.respond_to_auth_challenge(ClientId=self.client_id,
                                              ChallengeName=challenge_name)

    def sign_up(self, username, password, customer, role, tenants=None):
        custom_attr = [{
            'Name': 'name',
            'Value': username
        }, {
            'Name': CUSTOM_CUSTOMER_ATTR,
            'Value': customer
        }, {
            'Name': CUSTOM_ROLE_ATTR,
            'Value': role
        }]
        if tenants:
            custom_attr.append({
                'Name': CUSTOM_TENANTS_ATTR,
                'Value': tenants
            })
        validation_data = [
            {
                'Name': 'name',
                'Value': username
            }
        ]
        return self.client.sign_up(ClientId=self.client_id,
                                   Username=username,
                                   Password=password,
                                   UserAttributes=custom_attr,
                                   ValidationData=validation_data)

    def set_password(self, username, password, permanent=True):
        return self.client.admin_set_user_password(
            UserPoolId=self.user_pool_id, Username=username,
            Password=password, Permanent=permanent)

    def _get_user(self, username) -> Optional[dict]:
        users = self.client.list_users(
            UserPoolId=self.user_pool_id,
            Limit=1,
            Filter=f'username = "{username}"')['Users']
        if len(users) >= 1:
            return users[0]

    def is_user_exists(self, username) -> bool:
        return bool(self._get_user(username))

    def _get_user_attr(self, user, attr_name, query_user=True):
        """user attribute can be either a 'username' or a user dict object
        already fetched from AWS Cognito"""
        if query_user:
            user = self._get_user(username=user)
        for attr in user['Attributes']:
            if attr['Name'] == attr_name:
                return attr['Value']

    def get_user_role(self, username):
        return self._get_user_attr(username, CUSTOM_ROLE_ATTR)

    def get_user_customer(self, username):
        return self._get_user_attr(username, CUSTOM_CUSTOMER_ATTR)

    def get_user_tenants(self, username):
        return self._get_user_attr(username, CUSTOM_TENANTS_ATTR) or ''

    def update_role(self, username, role):
        role_attribute = [
            {
                'Name': CUSTOM_ROLE_ATTR,
                'Value': role
            }
        ]
        self.client.admin_update_user_attributes(UserPoolId=self.user_pool_id,
                                                 Username=username,
                                                 UserAttributes=role_attribute)

    def get_user_latest_login(self, username):
        return self._get_user_attr(username, CUSTOM_LATEST_LOGIN_ATTR)

    def update_latest_login(self, username: str):
        latest_login_attribute = [
            {
                'Name': CUSTOM_LATEST_LOGIN_ATTR,
                'Value': utc_iso()
            }
        ]
        self.client.admin_update_user_attributes(
            UserPoolId=self.user_pool_id, Username=username,
            UserAttributes=latest_login_attribute)

    def update_customer(self, username, customer):
        customer_attribute = [
            {
                'Name': CUSTOM_CUSTOMER_ATTR,
                'Value': customer
            }
        ]
        self.client.admin_update_user_attributes(
            UserPoolId=self.user_pool_id, Username=username,
            UserAttributes=customer_attribute)

    def update_tenants(self, username: str, tenants: Union[str, list]):
        if isinstance(tenants, list):
            tenants = ','.join(tenants)
        tenants_attribute = [
            {
                'Name': CUSTOM_TENANTS_ATTR,
                'Value': tenants
            }
        ]
        self.client.admin_update_user_attributes(
            UserPoolId=self.user_pool_id, Username=username,
            UserAttributes=tenants_attribute)

    def delete_role(self, username):
        self.client.admin_delete_user_attributes(
            UserPoolId=self.user_pool_id, Username=username,
            UserAttributeNames=[CUSTOM_ROLE_ATTR])

    def delete_customer(self, username):
        self.client.admin_delete_user_attributes(
            UserPoolId=self.user_pool_id, Username=username,
            UserAttributeNames=[CUSTOM_CUSTOMER_ATTR])

    def delete_tenants(self, username):
        self.client.admin_delete_user_attributes(
            UserPoolId=self.user_pool_id, Username=username,
            UserAttributeNames=[CUSTOM_TENANTS_ATTR])

    def is_system_user_exists(self):
        """Checks whether user with customer=$SYSTEM_CUSTOMER already exists"""
        users = self._list_users(attributes_to_get=[CUSTOM_CUSTOMER_ATTR, ])
        for user in users['Users']:
            if self._get_user_attr(user, CUSTOM_CUSTOMER_ATTR,
                                   query_user=False) == SYSTEM_CUSTOMER:
                return True
        return False

    def get_system_user(self):
        """
        Returns the user with customer=$SYSTEM_CUSTOMER
         if exists, else - None
         """
        users = self._list_users(attributes_to_get=[CUSTOM_CUSTOMER_ATTR, ])
        for user in users['Users']:
            if self._get_user_attr(user, CUSTOM_CUSTOMER_ATTR,
                                   query_user=False) == SYSTEM_CUSTOMER:
                return user['Username']

    def get_customers_latest_logins(self, customers=None):
        customers = customers or []
        result = {}
        users = self._list_users()
        for user in users.get('Users', []):
            customer = self._get_user_attr(
                user, CUSTOM_CUSTOMER_ATTR, query_user=False)
            # may be either with military prefix or without
            latest_login = self._get_user_attr(
                user, CUSTOM_LATEST_LOGIN_ATTR, query_user=False)
            if not result.get(customer):
                result[customer] = latest_login
            elif latest_login and utc_datetime(result[customer]) < \
                    utc_datetime(latest_login):
                result[customer] = latest_login
        if customers:
            result = {k: v for k, v in result.items() if k in customers}
        return result
