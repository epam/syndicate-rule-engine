from abc import ABC, abstractmethod
from datetime import datetime
from typing import Iterator, TYPE_CHECKING
from typing_extensions import Self, TypedDict

from helpers.time_helper import utc_datetime, utc_iso

if TYPE_CHECKING:
    from models.user import User


class UserWrapper:
    __slots__ = (
        'id',
        'username',
        'customer',
        'role',
        'latest_login',
        'created_at',
    )

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
            created_at=ca,
        )

    def get_dto(self) -> dict:
        return {
            'username': self.username,
            'customer': self.customer,
            'role': self.role,
            'latest_login': utc_iso(
                self.latest_login) if self.latest_login else None,
            'created_at': utc_iso(self.created_at) if self.created_at else None,
        }


class UsersIterator(Iterator[UserWrapper]):
    next_token: str | int | None = None

    def __iter__(self):
        return self

    def __next__(self) -> UserWrapper:
        raise NotImplementedError


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
                    customer: str | None = None, role: str | None = None,
                    ) -> UserWrapper:
        pass

    def does_user_exist(self, username: str) -> bool:
        """
        Use only if you don't need the user's data
        :param username:
        :return:
        """
        return not not self.get_user_by_username(username)
