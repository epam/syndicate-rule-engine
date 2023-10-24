import re
import secrets
from http import HTTPStatus
from typing import Optional

from connections.auth_extension.base_auth_client import BaseAuthClient
from helpers import CustodianException
from helpers.log_helper import get_logger

_LOG = get_logger(__name__)

SSM_NOT_AVAILABLE = re.compile(r'[^a-zA-Z0-9\/_.-]')


class CognitoUserService:

    def __init__(self, client: BaseAuthClient):
        self.client = client

    def save(self, username, password, customer, role, tenants=None):
        _LOG.debug(f'Validating password for user {username}')
        if self.client.is_user_exists(username):
            raise CustodianException(
                code=HTTPStatus.BAD_REQUEST,
                content=f'The user with name {username} already exists.')

        _LOG.debug(f'Creating the user with username {username}')
        if isinstance(tenants, list):
            tenants = ','.join(tenants)
        self.client.sign_up(username=username,
                            password=password,
                            customer=customer,
                            role=role,
                            tenants=tenants)
        _LOG.debug(f'Setting the password for the user {username}')
        self.client.set_password(username=username,
                                 password=password)
        _LOG.debug(f'Saving to table tenants {tenants}')

    @staticmethod
    def safe_name(name: str) -> str:
        return str(re.sub(SSM_NOT_AVAILABLE, '-', name))

    def generate_username(self, name: Optional[str] = 'custodian') -> str:
        """
        Generates unique username
        """
        return f'{self.safe_name(name)}-{secrets.token_urlsafe(4)}'

    def get_user_role_name(self, user):
        return self.client.get_user_role(user)

    def get_user_customer(self, user):
        return self.client.get_user_customer(user)

    def get_user_tenants(self, user):
        return self.client.get_user_tenants(user)

    def initiate_auth(self, username, password):
        return self.client.admin_initiate_auth(username=username,
                                               password=password)

    def respond_to_auth_challenge(self, challenge_name):
        return self.client.respond_to_auth_challenge(
            challenge_name=challenge_name)

    def update_role(self, username, role):
        self.client.update_role(username=username, role=role)

    def update_customer(self, username, customer):
        self.client.update_customer(username=username, customer=customer)

    def update_tenants(self, username, tenants):
        self.client.update_tenants(username=username, tenants=tenants)

    def is_user_exists(self, username):
        return self.client.is_user_exists(username)

    def delete_role(self, username):
        self.client.delete_role(username=username)

    def delete_tenants(self, username):
        self.client.delete_tenants(username=username)

    def delete_customer(self, username):
        self.client.delete_customer(username=username)

    def is_system_user_exists(self):
        return self.client.is_system_user_exists()

    def get_system_user(self):
        return self.client.get_system_user()

    def get_customers_latest_logins(self, customers: list = None):
        return self.client.get_customers_latest_logins(customers)

    def admin_delete_user(self, username: str):
        self.client.admin_delete_user(username)

    def set_password(self, username: str, password: str):
        self.client.set_password(username, password)
