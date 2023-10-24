from http import HTTPStatus

from handlers.abstracts.abstract_user_handler import AbstractUserHandler
from helpers import build_response
from helpers.constants import HTTPMethod, ROLE_ATTR
from helpers.log_helper import get_logger
from services.rbac.iam_cache_service import CachedIamService
from services.user_service import CognitoUserService

_LOG = get_logger(__name__)


class UserRoleHandler(AbstractUserHandler):
    """
    Manage User Role API
    """
    def __init__(self, user_service: CognitoUserService,
                 cached_iam_service: CachedIamService):
        self.user_service = user_service
        self.iam_service = cached_iam_service

    def define_action_mapping(self):
        return {
            '/users/role': {
                HTTPMethod.GET: self.get_role_attribute,
                HTTPMethod.POST: self.set_role_attribute,
                HTTPMethod.PATCH: self.update_role_attribute,
                HTTPMethod.DELETE: self.delete_role_attribute
            }
        }

    def get_attribute_value(self, username: str):
        return self.user_service.get_user_role_name(user=username)

    def check_user_exist(self, username: str):
        return self.user_service.is_user_exists(username=username)

    def update_attribute(self, username: str, role: str):
        return self.user_service.update_role(username=username,
                                             role=role)

    def validate_value(self, event: dict):
        target_user = event.get('target_user')
        attribute_value = event.get(self.attribute_name)
        customer_display_name = self.user_service.get_user_customer(
            target_user)
        if not self.iam_service.get_role(
                customer=customer_display_name,
                name=attribute_value):
            _LOG.error(f'Invalid value for attribute {self.attribute_name}: '
                       f'{attribute_value}')
            return build_response(
                code=HTTPStatus.BAD_REQUEST,
                content=f'Invalid value for attribute {self.attribute_name}: '
                        f'{attribute_value}')

    def delete_attribute(self, username: str):
        return self.user_service.delete_role(username=username)

    @property
    def attribute_name(self):
        return ROLE_ATTR

    def get_role_attribute(self, event):
        return self._basic_get_user_attribute_handler(event=event)

    def set_role_attribute(self, event):
        return self._basic_set_user_attribute_handler(event=event)

    def update_role_attribute(self, event):
        return self._basic_update_user_attribute_handler(event=event)

    def delete_role_attribute(self, event):
        return self._basic_delete_user_attribute_handler(event=event)
