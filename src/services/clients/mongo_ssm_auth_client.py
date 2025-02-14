from datetime import timedelta
from http import HTTPStatus
import json
import secrets
from typing import cast

import bcrypt
from jwcrypto import jwt
from pymongo import MongoClient
from pynamodb.pagination import ResultIterator

from helpers.constants import (
    CAASEnv,
    COGNITO_SUB,
    COGNITO_USERNAME,
    CUSTOM_CUSTOMER_ATTR,
    CUSTOM_LATEST_LOGIN_ATTR,
    CUSTOM_ROLE_ATTR,
    CUSTOM_TENANTS_ATTR,
    PRIVATE_KEY_SECRET_NAME,
)
from helpers.lambda_response import ResponseFactory
from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime, utc_iso
from models import MONGO_CLIENT
from models.user import User
from services.clients.cognito import (
    AuthenticationResult,
    BaseAuthClient,
    UserWrapper,
    UsersIterator,
)
from services.clients.jwt_management_client import JWTManagementClient
from services.clients.ssm import AbstractSSMClient

_LOG = get_logger(__name__)

EXPIRATION_IN_MINUTES = 60

TOKEN_EXPIRED_MESSAGE = 'The incoming token has expired'
UNAUTHORIZED_MESSAGE = 'Unauthorized'


class MongoAndSSMUsersIterator(UsersIterator):
    __slots__ = '_it',

    def __init__(self, it: ResultIterator[User]):
        self._it = it

    @property
    def next_token(self):
        return self._it.last_evaluated_key

    def __next__(self) -> UserWrapper:
        return UserWrapper.from_user_model(self._it.__next__())


class MongoAndSSMAuthClient(BaseAuthClient):
    __slots__ = '_ssm', '_jwt_client', '_refresh_col'

    def __init__(self, ssm_client: AbstractSSMClient):
        self._ssm = ssm_client
        self._jwt_client = None
        self._refresh_col = cast(MongoClient, MONGO_CLIENT).get_database(
            CAASEnv.MONGO_DATABASE.get()
        ).get_collection('CaaSRefreshTokenChains')

    @property
    def jwt_client(self) -> JWTManagementClient:
        if self._jwt_client:
            return self._jwt_client
        jwk_pem = self._ssm.get_secret_value(PRIVATE_KEY_SECRET_NAME)
        unavailable = ResponseFactory(HTTPStatus.SERVICE_UNAVAILABLE).default()

        if not jwk_pem or not isinstance(jwk_pem, str):
            _LOG.error('Can not find jwt-secret')
            raise unavailable.exc()
        try:
            cl = JWTManagementClient.from_b64_pem(jwk_pem)
            self._jwt_client = cl
            return cl
        except ValueError:
            raise unavailable.exc()

    def get_user_by_username(self, username: str) -> UserWrapper | None:
        item = User.get_nullable(hash_key=username)
        if not item:
            return
        return UserWrapper.from_user_model(item)

    def query_users(self, customer: str | None = None,
                    limit: int | None = None,
                    next_token: str | dict | None = None) -> UsersIterator:
        fc = None
        if customer:
            fc = (User.customer == customer)
        it = User.scan(
            limit=limit,
            last_evaluated_key=next_token,
            filter_condition=fc
        )
        return MongoAndSSMUsersIterator(it)

    def set_user_password(self, username: str, password: str) -> bool:
        User(user_id=username).update(actions=[
            User.password.set(
                bcrypt.hashpw(password.encode(), bcrypt.gensalt()))
        ])
        return True

    @staticmethod
    def _update_password_attr(user: User, password: str):
        user.password = bcrypt.hashpw(password.encode(), bcrypt.gensalt())

    def update_user_attributes(self, user: UserWrapper):
        actions = []
        if user.customer:
            actions.append(User.customer.set(user.customer))
        if user.role:
            actions.append(User.role.set(user.role))
        if user.latest_login:
            actions.append(User.latest_login.set(utc_iso(user.latest_login)))
        if actions:
            User(user_id=user.username).update(actions=actions)

    def delete_user(self, username: str) -> None:
        User(user_id=username).delete()

    @staticmethod
    def _gen_refresh_token_version() -> str:
        return secrets.token_hex()

    def _gen_refresh_token(self, username: str, version: str) -> str:
        t = self.jwt_client.sign({'username': username, 'version': version})
        return self.jwt_client.encrypt(t)

    def _decrypt_refresh_token(self, token: str) -> tuple[str, str] | None:
        t = self.jwt_client.decrypt(token)
        if not t:
            return
        try:
            t = self.jwt_client.verify(t.claims)
        except Exception:
            return
        dct = json.loads(t.claims)
        return dct['username'], dct['version']

    def _gen_access_token(self, user: User) -> str:
        return self.jwt_client.sign(
            claims={
                COGNITO_USERNAME: user.user_id,
                COGNITO_SUB: str(user.__mongo_id__),
                CUSTOM_CUSTOMER_ATTR: user.customer,
                CUSTOM_TENANTS_ATTR: user.tenants or '',
                CUSTOM_ROLE_ATTR: user.role,
                CUSTOM_LATEST_LOGIN_ATTR: user.latest_login,
            },
            exp=timedelta(minutes=EXPIRATION_IN_MINUTES)
        )

    def authenticate_user(self, username: str, password: str
                          ) -> AuthenticationResult | None:
        user_item = User.get_nullable(hash_key=username)
        if not user_item:
            return
        check = bcrypt.checkpw(
            password=password.encode(),
            hashed_password=user_item.password
        )
        if not check:
            _LOG.info('Invalid password provided by user')
            return
        token = self._gen_access_token(user_item)

        rt_version = self._gen_refresh_token_version()
        refresh_token = self._gen_refresh_token(username, rt_version)
        self._refresh_col.replace_one({'_id': username}, {
            'v': rt_version  # latest version for user
        }, upsert=True)

        # that id_token is actually used as access_token. But because Api Gw
        # required cognito id_token to be passed, we keep here the similar
        # interface to the client inside ./cognito.py
        return {
            'id_token': token,
            'refresh_token': refresh_token,
            'expires_in': EXPIRATION_IN_MINUTES * 60
        }

    def refresh_token(self, refresh_token: str) -> AuthenticationResult | None:
        _LOG.info('Starting on-prem refresh token flow')
        tpl = self._decrypt_refresh_token(refresh_token)
        if not tpl:
            _LOG.info('Invalid refresh token provided. Cannot refresh')
            return
        username, rt_version = tpl
        latest = self._refresh_col.find_one({'_id': username})
        if not latest or not latest.get('v'):
            _LOG.warning('Latest version of token not found in DB '
                         'but valid token was received. Cannot refresh')
            return
        correct_version = latest['v']
        if rt_version != correct_version:
            _LOG.warning('Valid token received but its version and one from '
                         'DB do not match. Stolen refresh token or user '
                         'reused one. Invalidating existing version')
            self._refresh_col.delete_one({'_id': username})
            return
        rt_version = self._gen_refresh_token_version()
        self._refresh_col.replace_one({'_id': username}, {
            'v': rt_version  # latest version for user
        }, upsert=True)

        user_item = User.get_nullable(hash_key=username)
        return {
            'id_token': self._gen_access_token(user_item),
            'refresh_token': self._gen_refresh_token(username, rt_version),
            'expires_in': EXPIRATION_IN_MINUTES * 60
        }

    def signup_user(self, username: str, password: str,
                    customer: str | None = None, role: str | None = None
                    ) -> UserWrapper:
        created_at = utc_datetime()
        user = User(
            user_id=username,
            customer=customer,
            role=role,
            created_at=utc_iso(created_at)
        )
        self._update_password_attr(user, password)
        user.save()
        return UserWrapper(
            username=username,
            customer=customer,
            role=role,
            created_at=created_at
        )

    def decode_token(self, token: str) -> dict:
        try:
            verified = self.jwt_client.verify(token)
        except jwt.JWTExpired:
            _LOG.warning('Access token has expired')
            raise ResponseFactory(HTTPStatus.UNAUTHORIZED).message(
                TOKEN_EXPIRED_MESSAGE).exc()
        except (jwt.JWException, ValueError, Exception) as e:
            _LOG.warning(f'Could not decode token: {e}')
            raise ResponseFactory(HTTPStatus.UNAUTHORIZED).message(
                UNAUTHORIZED_MESSAGE).exc()
        return json.loads(verified.claims)
