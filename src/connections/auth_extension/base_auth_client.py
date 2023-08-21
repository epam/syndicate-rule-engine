from abc import ABC, abstractmethod
from typing import Union


class BaseAuthClient(ABC):
    @abstractmethod
    def admin_initiate_auth(self, username: str, password: str) -> dict:
        ...

    @abstractmethod
    def admin_delete_user(self, username: str):
        ...

    @abstractmethod
    def respond_to_auth_challenge(self, challenge_name: str):
        ...

    @abstractmethod
    def sign_up(self, username, password, customer, role, tenants=None):
        ...

    @abstractmethod
    def set_password(self, username, password, permanent=True):
        ...

    @abstractmethod
    def is_user_exists(self, username: str) -> bool:
        ...

    @abstractmethod
    def get_user_role(self, username: str):
        ...

    @abstractmethod
    def get_user_customer(self, username: str):
        ...

    @abstractmethod
    def get_user_tenants(self, username: str):
        ...

    @abstractmethod
    def update_role(self, username: str, role: str):
        ...

    @abstractmethod
    def get_user_latest_login(self, username: str):
        ...

    @abstractmethod
    def update_latest_login(self, username: str):
        ...

    @abstractmethod
    def update_customer(self, username: str, customer: str):
        ...

    @abstractmethod
    def update_tenants(self, username: str, tenants: Union[str, list]):
        ...

    @abstractmethod
    def delete_role(self, username: str):
        ...

    @abstractmethod
    def delete_customer(self, username: str):
        ...

    @abstractmethod
    def delete_tenants(self, username: str):
        ...

    @abstractmethod
    def is_system_user_exists(self) -> bool:
        ...

    @abstractmethod
    def get_system_user(self):
        ...

    @abstractmethod
    def get_customers_latest_logins(self, customers=None):
        ...
