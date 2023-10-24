from handlers.abstracts.abstract_user_handler import AbstractUserHandler
from helpers import build_response
from http import HTTPStatus
from helpers.constants import CUSTOMER_ATTR, HTTPMethod
from helpers.log_helper import get_logger
from services.modular_service import ModularService
from services.user_service import CognitoUserService

_LOG = get_logger(__name__)


class UserCustomerHandler(AbstractUserHandler):
    """
    Manage User Customer API
    """

    def __init__(self, modular_service: ModularService,
                 user_service: CognitoUserService):
        self.modular_service = modular_service
        self.user_service = user_service

    def define_action_mapping(self):
        return {
            '/users/customer': {
                HTTPMethod.GET: self.get_customer_attribute,
                HTTPMethod.POST: self.set_customer_attribute,
                HTTPMethod.PATCH: self.update_customer_attribute,
                HTTPMethod.DELETE: self.delete_customer_attribute
            }
        }

    def get_attribute_value(self, username: str):
        return self.user_service.get_user_customer(user=username)

    def check_user_exist(self, username: str):
        return self.user_service.is_user_exists(username=username)

    def update_attribute(self, username: str, customer: str):
        return self.user_service.update_customer(username=username,
                                                 customer=customer)

    def validate_value(self, event: dict):
        attribute_value = event.get(self.attribute_name)

        if not self.modular_service.get_customer(customer=attribute_value):
            _LOG.error(f'Invalid value for attribute {self.attribute_name}: '
                       f'{attribute_value}')
            return build_response(
                code=HTTPStatus.BAD_REQUEST,
                content=f'Invalid value for attribute {self.attribute_name}: '
                        f'{attribute_value}')

        return True

    def delete_attribute(self, username: str):
        return self.user_service.delete_customer(username=username)

    @property
    def attribute_name(self):
        return CUSTOMER_ATTR

    def get_customer_attribute(self, event):
        return self._basic_get_user_attribute_handler(event=event)

    def set_customer_attribute(self, event):
        return self._basic_set_user_attribute_handler(event=event)

    def update_customer_attribute(self, event):
        return self._basic_update_user_attribute_handler(event=event)

    def delete_customer_attribute(self, event):
        return self._basic_delete_user_attribute_handler(event=event)
