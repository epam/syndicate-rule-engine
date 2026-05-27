from abc import ABC, abstractmethod
from datetime import datetime
from functools import cached_property
from http import HTTPStatus
from typing import Generator, Iterator, TYPE_CHECKING
from typing_extensions import NotRequired, Self, TypedDict

from botocore.exceptions import ClientError

from helpers.constants import (
    CUSTOM_CUSTOMER_ATTR,
    CUSTOM_LATEST_LOGIN_ATTR,
    CUSTOM_ROLE_ATTR,
)
from helpers.lambda_response import ResponseFactory
from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime, utc_iso
from services.environment_service import EnvironmentService
from services.clients import Boto3ClientFactory

if TYPE_CHECKING:
    from models.user import User
    from botocore.client import BaseClient

_LOG = get_logger(__name__)


class _CognitoUserAttr(TypedDict):
    Name: str
    Value: str


class CognitoUserModel(TypedDict):
    Username: str
    UserAttributes: NotRequired[list[_CognitoUserAttr]]  # either this one
    Attributes: NotRequired[list[_CognitoUserAttr]]  # or this one
    UserCreateDate: datetime
    UserLastModifiedDate: datetime
    Enabled: bool


class UserWrapper:
    __slots__ = ('id', 'username', 'customer', 'role', 'latest_login',
                 'created_at')

    def __init__(self, username: str, customer: str | None = None,
                 role: str | None = None, latest_login: datetime | None = None,
                 created_at: datetime | None = None, sub: str | None = None):
        """
        Sub is not used currently, so it's not important. Username represents
        user id
        :param username:
        :param customer:
        :param role:
        :param latest_login:
        :param created_at:
        :param sub:
        """
        self.username = username
        self.customer = customer
        self.role = role
        self.latest_login = latest_login
        self.created_at = created_at
        self.id = sub

    @classmethod
    def from_user_model(cls, user: 'User') -> Self:
        ll = None
        if user.latest_login:
            ll = utc_datetime(user.latest_login)
        ca = None
        if user.created_at:
            ca = utc_datetime(user.created_at)
        return cls(
            sub=str(user.__mongo_id__),
            username=user.user_id,
            customer=user.customer,
            role=user.role,
            latest_login=ll,
            created_at=ca
        )

    @classmethod
    def from_cognito_model(cls, model: CognitoUserModel) -> Self:
        attrs = model.get('UserAttributes') or model.get('Attributes') or ()
        attributes = {a['Name']: a['Value'] for a in attrs}
        ll = None
        if item := attributes.get(CUSTOM_LATEST_LOGIN_ATTR):
            ll = utc_datetime(item)
        return cls(
            sub=attributes.get('sub'),  # valid for onprem
            username=model['Username'],
            customer=attributes.get(CUSTOM_CUSTOMER_ATTR),
            role=attributes.get(CUSTOM_ROLE_ATTR),
            latest_login=ll,
            created_at=model['UserCreateDate']
        )

    def get_dto(self) -> dict:
        return {
            'username': self.username,
            'customer': self.customer,
            'role': self.role,
            'latest_login': utc_iso(
                self.latest_login) if self.latest_login else None,
            'created_at': utc_iso(self.created_at) if self.created_at else None
        }


class UsersIterator(Iterator[UserWrapper]):
    next_token: str | int | None = None

    def __iter__(self):
        return self

    def __next__(self) -> UserWrapper:
        raise NotImplementedError


class CognitoUsersIterator(UsersIterator):
    __slots__ = '_cl', '_upi', '_customer', '_limit', 'next_token'

    def __init__(self, client: 'BaseClient', user_pool_id: str,
                 customer: str | None = None,
                 limit: int | None = None, next_token: str | None = None):
        self._cl = client
        self._upi = user_pool_id
        self._customer = customer

        self._limit = limit
        self.next_token = next_token

    def _get_next_page(self, limit: int | None = None,
                       token: str | None = None
                       ) -> tuple[list[CognitoUserModel], str | None]:
        params = dict(UserPoolId=self._upi)
        if limit:
            params['Limit'] = limit
        if token:
            params['PaginationToken'] = token
        try:
            res = self._cl.list_users(**params)
            return res.get('Users') or [], res.get('PaginationToken')
        except ClientError:
            _LOG.warning('Unexpected error occurred listing users',
                         exc_info=True)
            return [], None

    def __iter__(self) -> Generator[UserWrapper, None, None]:
        # local vars
        _limit = self._limit
        first = True
        customer = self._customer
        while _limit != 0 and (first or self.next_token):
            res = self._get_next_page(_limit, self.next_token)
            first = False
            self.next_token = res[1]
            for user in map(UserWrapper.from_cognito_model, res[0]):
                if customer and user.customer != customer:
                    continue
                yield user
                if _limit is not None:
                    _limit -= 1


class AuthenticationResult(TypedDict):
    id_token: str
    refresh_token: str | None
    expires_in: int


class BaseAuthClient(ABC):
    @abstractmethod
    def get_user_by_username(self, username: str) -> UserWrapper | None:
        pass

    @abstractmethod
    def query_users(self, customer: str | None = None,
                    limit: int | None = None,
                    next_token: str | dict | None = None) -> UsersIterator:
        pass

    @abstractmethod
    def set_user_password(self, username: str, password: str) -> bool:
        pass

    @abstractmethod
    def update_user_attributes(self, user: UserWrapper):
        """
        Updates all the attributes that are not equal to None in user wrapper
        :param user:
        :return:
        """

    @abstractmethod
    def delete_user(self, username: str) -> None:
        pass

    @abstractmethod
    def authenticate_user(self, username: str, password: str
                          ) -> AuthenticationResult | None:
        pass

    @abstractmethod
    def refresh_token(self, refresh_token: str) -> AuthenticationResult | None:
        pass

    @abstractmethod
    def signup_user(self, username: str, password: str,
                    customer: str | None = None, role: str | None = None) -> UserWrapper:
        pass

    def does_user_exist(self, username: str) -> bool:
        """
        Use only if you don't need the user's data
        :param username:
        :return:
        """
        return not not self.get_user_by_username(username)


class CognitoClient(BaseAuthClient):
    def __init__(self, environment_service: EnvironmentService):
        self._env = environment_service

    @cached_property
    def client(self):
        return Boto3ClientFactory('cognito-idp').build(region_name=self._env.aws_region())

    @property
    def user_pool_name(self) -> str:
        return self._env.get_user_pool_name()

    @cached_property
    def user_pool_id(self) -> str:
        _LOG.info('Retrieving user pool id')
        _id = self._env.get_user_pool_id()
        if not _id:
            _LOG.warning('User pool id is not found in envs. '
                         'Scanning all the available pools to get the id')
            _id = self._pool_id_from_name(self.user_pool_name)
        if not _id:
            _message = 'Application Authentication Service is ' \
                       'not configured properly.'
            _LOG.error(f'User pool \'{self.user_pool_name}\' does '
                       f'not exists. {_message}')
            raise ResponseFactory(HTTPStatus.SERVICE_UNAVAILABLE).message(
                _message).exc()
        return _id

    @property
    def client_id(self) -> str:
        client = self.client.list_user_pool_clients(
            UserPoolId=self.user_pool_id, MaxResults=1)['UserPoolClients']
        if not client:
            _message = 'Application Authentication Service is not ' \
                       'configured properly: no client applications found'
            _LOG.error(_message)
            raise ResponseFactory(HTTPStatus.SERVICE_UNAVAILABLE).message(
                _message
            ).exc()
        return client[0]['ClientId']

    def _pool_id_from_name(self, name: str) -> str | None:
        """
        Since AWS Cognito can have two different pools with equal names,
        this method returns the first pool id which will be found.
        """
        for pool in self._list_user_pools():
            if pool['Name'] == name:
                return pool['Id']

    def _list_user_pools(self) -> Generator[dict, None, None]:
        first = True
        params = dict(MaxResults=10)
        while params.get('NextToken') or first:
            pools = self.client.list_user_pools(**params)
            yield from pools.get('UserPools') or []
            params['NextToken'] = pools.get('NextToken')
            if first:
                first = False

    def get_user_by_username(self, username: str) -> UserWrapper | None:
        try:
            item = self.client.admin_get_user(
                UserPoolId=self.user_pool_id,
                Username=username
            )
            _LOG.debug(f'Result of admin_get_user: {item}')
            return UserWrapper.from_cognito_model(item)
        except ClientError as e:
            _LOG.warning(f'ClientError occurred querying a user, {e}')
            return

    def query_users(self, customer: str | None = None,
                    limit: int | None = None,
                    next_token: str | None = None) -> UsersIterator:
        return CognitoUsersIterator(
            client=self.client,
            user_pool_id=self.user_pool_id,
            limit=limit,
            next_token=next_token,
            customer=customer
        )

    def set_user_password(self, username: str, password: str) -> bool:
        try:
            self.client.admin_set_user_password(
                UserPoolId=self.user_pool_id,
                Username=username,
                Password=password,
                Permanent=True
            )
            return True
        except ClientError:
            _LOG.warning('Could not set user password due to client error',
                         exc_info=True)
            return False

    def update_user_attributes(self, user: UserWrapper):
        def attr(n, v):
            return dict(Name=n, Value=v)

        attributes = []
        if user.customer:
            attributes.append(attr(CUSTOM_CUSTOMER_ATTR, user.customer))
        if user.role:
            attributes.append(attr(CUSTOM_ROLE_ATTR, user.customer))
        if user.latest_login:
            attributes.append(attr(CUSTOM_LATEST_LOGIN_ATTR,
                                   utc_iso(user.latest_login)))
        if attributes:
            self.client.admin_update_user_attributes(
                UserPoolId=self.user_pool_id,
                Username=user.username,
                UserAttributes=attributes
            )

    def delete_user(self, username: str) -> None:
        try:
            self.client.admin_delete_user(
                UserPoolId=self.user_pool_id,
                Username=username
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'UserNotFoundException':
                pass
            raise e

    def authenticate_user(self, username: str, password: str
                          ) -> AuthenticationResult | None:
        try:
            r = self.client.admin_initiate_auth(
                UserPoolId=self.user_pool_id,
                ClientId=self.client_id,
                AuthFlow='ADMIN_NO_SRP_AUTH',
                AuthParameters={'USERNAME': username, 'PASSWORD': password}
            )
            return {
                'id_token': r['AuthenticationResult']['IdToken'],
                'refresh_token': r['AuthenticationResult'].get('RefreshToken'),
                'expires_in': r['AuthenticationResult']['ExpiresIn']
            }
        except self.client.exceptions.UserNotFoundException:
            return
        except self.client.exceptions.NotAuthorizedException:
            return

    def refresh_token(self, refresh_token: str) -> AuthenticationResult | None:
        try:
            r = self.client.admin_initiate_auth(
                UserPoolId=self.user_pool_id,
                ClientId=self.client_id,
                AuthFlow='REFRESH_TOKEN_AUTH',
                AuthParameters={'REFRESH_TOKEN': refresh_token}
            )
            return {
                'id_token': r['AuthenticationResult']['IdToken'],
                'refresh_token': r['AuthenticationResult'].get('RefreshToken'),
                'expires_in': r['AuthenticationResult']['ExpiresIn']
            }
        except ClientError:
            _LOG.warning('Client error occurred trying to refresh token',
                         exc_info=True)

    def signup_user(self, username: str, password: str,
                    customer: str | None = None, role: str | None = None
                    ) -> UserWrapper:
        def attr(n, v):
            return dict(Name=n, Value=v)

        attrs = [attr('name', username)]
        if customer:
            attrs.append(attr(CUSTOM_CUSTOMER_ATTR, customer))
        if role:
            attrs.append(attr(CUSTOM_ROLE_ATTR, role))
        validation_data = [attr('name', username)]
        res = self.client.sign_up(
            ClientId=self.client_id,
            Username=username,
            Password=password,
            UserAttributes=attrs,
            ValidationData=validation_data
        )
        self.client.admin_set_user_password(
            UserPoolId=self.user_pool_id,
            Username=username,
            Password=password,
            Permanent=True
        )
        return UserWrapper(
            username=username,
            customer=customer,
            role=role,
            created_at=utc_datetime(),
            sub=res['UserSub'],
        )
