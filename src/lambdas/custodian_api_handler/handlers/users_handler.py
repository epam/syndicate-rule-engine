from functools import cached_property
from http import HTTPStatus
from typing import cast

from botocore.exceptions import ClientError
from modular_sdk.models.customer import Customer
from modular_sdk.services.customer_service import CustomerService

from handlers import AbstractHandler, Mapping
from helpers import NextToken
from helpers.constants import CustodianEndpoint, HTTPMethod, Permission
from helpers.lambda_response import ResponseFactory, build_response
from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime
from models.policy import PolicyEffect
from services import SP
from services.abs_lambda import ProcessedEvent
from services.clients.cognito import BaseAuthClient, UserWrapper
from services.rbac_service import PolicyService, RoleService
from validators.swagger_request_models import (
    BaseModel,
    BasePaginationModel,
    RefreshPostModel,
    SignInPostModel,
    SignUpModel,
    UserPatchModel,
    UserPostModel,
    UserResetPasswordModel,
)
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)


class UsersHandler(AbstractHandler):
    def __init__(self, user_client: BaseAuthClient,
                 customer_service: CustomerService,
                 role_service: RoleService, policy_service: PolicyService):
        self._user_client = user_client
        self._cs = customer_service
        self._rs = role_service
        self._ps = policy_service

    @classmethod
    def build(cls) -> 'UsersHandler':
        return cls(
            user_client=SP.users_client,
            customer_service=SP.modular_client.customer_service(),
            role_service=SP.role_service,
            policy_service=SP.policy_service
        )

    @cached_property
    def mapping(self) -> Mapping:
        return {
            CustodianEndpoint.USERS_RESET_PASSWORD: {
                HTTPMethod.POST: self.reset_password
            },
            CustodianEndpoint.USERS_WHOAMI: {
                HTTPMethod.GET: self.whoami
            },
            CustodianEndpoint.USERS: {
                HTTPMethod.GET: self.query,
                HTTPMethod.POST: self.post
            },
            CustodianEndpoint.USERS_USERNAME: {
                HTTPMethod.GET: self.get,
                HTTPMethod.PATCH: self.patch,
                HTTPMethod.DELETE: self.delete
            },
            CustodianEndpoint.SIGNUP: {
                HTTPMethod.POST: self.signup
            },
            CustodianEndpoint.SIGNIN: {
                HTTPMethod.POST: self.signin
            },
            CustodianEndpoint.REFRESH: {
                HTTPMethod.POST: self.refresh,
            },
        }

    @validate_kwargs
    def query(self, event: BasePaginationModel):
        cursor = self._user_client.query_users(
            customer=event.customer,
            limit=event.limit,
            next_token=NextToken.deserialize(event.next_token).value
        )
        items = list(cursor)
        return ResponseFactory().items(
            it=(i.get_dto() for i in items),
            next_token=NextToken(cursor.next_token)
        ).build()

    @validate_kwargs
    def get(self, event: BaseModel, username: str):
        item = self._user_client.get_user_by_username(
            username
        )
        if not item or event.customer and item.customer != event.customer:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                'User not found'
            ).exc()
        return build_response(item.get_dto())

    @validate_kwargs
    def post(self, event: UserPostModel):
        if self._user_client.does_user_exist(event.username):
            raise ResponseFactory(HTTPStatus.CONFLICT).message(
                f'User with such username already exists'
            ).exc()
        if not self._rs.get_nullable(event.customer_id, event.role_name):
            raise ResponseFactory(HTTPStatus.BAD_REQUEST).message(
                f'Role {event.role_name} not found in customer '
                f'{event.customer_id}'
            ).exc()

        user = self._user_client.signup_user(
            username=event.username,
            password=event.password,
            customer=event.customer_id,
            role=event.role_name
        )
        # seems like we need this additional step for cognito
        self._user_client.set_user_password(event.username, event.password)
        return build_response(user.get_dto())

    @validate_kwargs
    def patch(self, event: UserPatchModel, username: str):
        item = self._user_client.get_user_by_username(username)
        if not item or event.customer_id and item.customer != event.customer_id:
            raise ResponseFactory(HTTPStatus.NOT_FOUND).message(
                'User not found'
            ).exc()
        params = dict()
        if event.role_name:
            params['role'] = event.role_name
            item.role = event.role_name
        to_update = UserWrapper(
            username=username,
            **params
        )
        self._user_client.update_user_attributes(to_update)
        if event.password:
            _LOG.info('Password was provided. Updating user password')
            self._user_client.set_user_password(username, event.password)

        return build_response(item.get_dto())

    @validate_kwargs
    def delete(self, event: BaseModel, username: str):
        user = self._user_client.get_user_by_username(username)
        if not user or event.customer_id and user.customer != event.customer_id:
            return build_response(code=HTTPStatus.NO_CONTENT)
        # users exists and it belongs to this customer
        self._user_client.delete_user(username)
        return build_response(code=HTTPStatus.NO_CONTENT)

    @validate_kwargs
    def whoami(self, event: BaseModel, _pe: ProcessedEvent):
        username = cast(str, _pe['cognito_username'])
        _LOG.debug(f'Getting user by username {username}')
        item = self._user_client.get_user_by_username(username)
        assert item, 'Something strange happening, probably a bug'
        return build_response(item.get_dto())

    @validate_kwargs
    def signin(self, event: SignInPostModel):
        username = event.username
        password = event.password
        _LOG.info('Going to initiate the authentication flow')
        auth_result = self._user_client.authenticate_user(
            username=username,
            password=password
        )
        if not auth_result:
            raise ResponseFactory(HTTPStatus.UNAUTHORIZED).message(
                'Incorrect username and/or password'
            ).exc()
        self._user_client.update_user_attributes(UserWrapper(
            username=event.username,
            latest_login=utc_datetime()
        ))

        return ResponseFactory().raw({
            'access_token': auth_result['id_token'],
            'refresh_token': auth_result['refresh_token'],
            'expires_in': auth_result['expires_in']
        }).build()

    @validate_kwargs
    def signup(self, event: SignUpModel):
        if self._cs.get(event.customer_name):
            raise ResponseFactory(HTTPStatus.CONFLICT).message(
                f'Customer {event.customer_name} already exists'
            ).exc()
        if self._user_client.does_user_exist(event.username):
            raise ResponseFactory(HTTPStatus.CONFLICT).message(
                f'User {event.username} already exists'
            ).exc()
        customer = Customer(
            name=event.customer_name,
            display_name=event.customer_display_name,
            admins=list(event.customer_admins),
            is_active=True
        )
        policy = self._ps.create(
            customer=event.customer_name,
            name='admin_policy',
            permissions=sorted([i.value for i in Permission.iter_enabled()]),
            description='Auto-created policy for newly signed up user',
            effect=PolicyEffect.ALLOW,
            tenants=('*', )
        )
        role = self._rs.create(
            customer=event.customer_name,
            name='admin_role',
            expiration=None,
            policies=('admin_policy', ),
            description='Auto-created role for newly signed up user'
        )
        try:
            customer.save()
        except ClientError:
            _LOG.warning('Cannot save customer. Probably no permissions.',
                         exc_info=True)
            raise ResponseFactory(HTTPStatus.FORBIDDEN).message(
                'Cannot create a new user. Please, contact support'
            ).exc()
        self._rs.save(role)
        self._ps.save(policy)
        self._user_client.signup_user(
            username=event.username,
            password=event.password,
            role='admin_role',
            customer=event.customer_name
        )
        _LOG.debug(f'Saving user: {event.username}')
        return build_response(content=f'The user {event.username} was created')

    @validate_kwargs
    def refresh(self, event: RefreshPostModel):
        auth_result = self._user_client.refresh_token(event.refresh_token)
        if not auth_result:
            raise ResponseFactory(HTTPStatus.UNAUTHORIZED).default().exc()
        return ResponseFactory().raw({
            'access_token': auth_result['id_token'],
            'refresh_token': auth_result['refresh_token'],
            'expires_in': auth_result['expires_in']
        }).build()

    @validate_kwargs
    def reset_password(self, event: UserResetPasswordModel,
                       _pe: ProcessedEvent):
        username = cast(str, _pe['cognito_username'])
        self._user_client.set_user_password(username, event.new_password)
        return build_response(code=HTTPStatus.NO_CONTENT)
