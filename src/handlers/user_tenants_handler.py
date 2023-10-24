from http import HTTPStatus

from handlers.abstracts.abstract_user_handler import AbstractUserHandler
from helpers import build_response, validate_params
from helpers.constants import HTTPMethod, TENANTS_ATTR
from helpers.log_helper import get_logger
from services.modular_service import ModularService
from services.user_service import CognitoUserService

_LOG = get_logger(__name__)


class UserTenantsHandler(AbstractUserHandler):
    """
    Manage User Tenants API
    """

    def __init__(self, modular_service: ModularService,
                 user_service: CognitoUserService):
        self.modular_service = modular_service
        self.user_service = user_service

    def define_action_mapping(self):
        return {
            '/users/tenants': {
                HTTPMethod.GET: self.get_tenants_attribute,
                HTTPMethod.PATCH: self.update_tenants_attribute,
                HTTPMethod.DELETE: self.delete_tenants_attribute
            }
        }

    def get_attribute_value(self, username: str):
        return self.user_service.get_user_tenants(user=username)

    def check_user_exist(self, username: str):
        return self.user_service.is_user_exists(username=username)

    def update_attribute(self, username: str, tenants: str):
        return self.user_service.update_tenants(username=username,
                                                tenants=tenants)

    def validate_value(self, event: dict):
        target_user = event.get('target_user')
        if not target_user:
            target_user = event.get('user_id')

        user_customer = self.user_service.get_user_customer(target_user)
        attribute_value = event.get(self.attribute_name, [])
        result_value = attribute_value.copy()
        for t in attribute_value:
            tenant = self.modular_service.get_tenant(t)
            if not tenant:
                _LOG.warning(f'Invalid tenant name for attribute '
                             f'{self.attribute_name}: {t}')
                result_value.remove(t)
            elif tenant.attribute_values.get('customer_name') != user_customer:
                _LOG.warning(f'No tenant {t} within customer {user_customer}')
                result_value.remove(t)
            if len(result_value) == 0:
                return build_response(
                    code=HTTPStatus.BAD_REQUEST,
                    content=f'Invalid value for attribute '
                            f'{self.attribute_name}: {attribute_value}')
        return result_value

    def delete_attribute(self, username: str):
        return self.user_service.delete_tenants(username=username)

    @property
    def attribute_name(self):
        return TENANTS_ATTR

    def get_tenants_attribute(self, event):
        return self._basic_get_user_attribute_handler(event=event)

    def set_tenants_attribute(self, event):
        _LOG.debug(f'Create {self.attribute_name} attribute for user: {event}')
        validate_params(event=event, required_params_list=['user_id'])

        target_user = event.get('target_user')
        if not target_user:
            target_user = event.get('user_id')

        if self.get_attribute_value(target_user):
            _LOG.error(f'Attribute {self.attribute_name} for user '
                       f'{target_user} already exists')
            return build_response(
                code=HTTPStatus.BAD_REQUEST,
                content=f'Attribute {self.attribute_name} for user '
                        f'{target_user} already exists')

        attribute_value = event.get(self.attribute_name)
        if not attribute_value:
            _LOG.debug(
                f'Attribute value for the {self.attribute_name} attribute '
                f'is not specified')
            return build_response(
                code=HTTPStatus.BAD_REQUEST,
                content=f'Attribute value for the {self.attribute_name} '
                        f'attribute is not specified')
        # return True if value is valid
        attribute_value = self.validate_value(event=event)

        _LOG.debug(f'Creating {self.attribute_name} attribute')
        self.update_attribute(target_user, attribute_value)

        return build_response(
            code=HTTPStatus.OK,
            content=f'Attribute {self.attribute_name} has been added to user '
                    f'{target_user}.')

    def update_tenants_attribute(self, event):
        validate_params(event=event, required_params_list=['user_id'])
        target_user = event.get('target_user')
        if not target_user:
            target_user = event.get('user_id')
        self._validate_user_existence(username=target_user)

        existing_tenants = self.user_service.get_user_tenants(target_user)
        if not existing_tenants:
            return self.set_tenants_attribute(event=event)

        existing_tenants = existing_tenants.replace(' ', '').split(',')
        existing_tenants.extend(event.get(TENANTS_ATTR, []))
        event[TENANTS_ATTR] = list(set(existing_tenants))
        _LOG.debug(f'Update {self.attribute_name} attribute for user: '
                   f'{event}')
        # for what????
        # validate_params(event=event, required_params_list=['user_id'])
        #
        # self._validate_user_existence(username=target_user)

        if not self.get_attribute_value(target_user):
            _LOG.error(
                f'Attribute {self.attribute_name} for user {target_user} '
                f'does not exist')
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=f'Attribute {self.attribute_name} for user '
                        f'{target_user} does not exist')

        attribute_value = event.get(self.attribute_name)
        if not attribute_value:
            _LOG.error(
                f'Attribute value for the {self.attribute_name} attribute '
                f'is not specified')
            return build_response(
                code=HTTPStatus.BAD_REQUEST,
                content=f'Attribute value for the {self.attribute_name} '
                        f'attribute is not specified')

        attribute_value = self.validate_value(event=event)

        _LOG.debug(f'Updating {self.attribute_name} attribute')
        self.update_attribute(target_user, attribute_value)

        return build_response(
            code=HTTPStatus.OK,
            content={self.attribute_name: attribute_value})

    def delete_tenants_attribute(self, event):
        _LOG.debug(f'Delete {self.attribute_name} attribute for user: {event}')

        validate_params(event=event, required_params_list=['user_id'])
        target_user = event.get('target_user')
        if not target_user:
            target_user = event.get('user_id')
        self._validate_user_existence(username=target_user)

        if not event.get('all'):
            _LOG.debug(f'Removing some tenants from {self.attribute_name} '
                       f'attribute')
            existing_tenants = self.user_service.get_user_tenants(target_user)
            if not existing_tenants:
                return build_response(
                    code=HTTPStatus.OK,
                    content=f'Attribute {self.attribute_name} for user '
                            f'{target_user} is already empty.')

            tenants_to_delete = event.get(TENANTS_ATTR, [])
            existing_tenants = existing_tenants.replace(' ', '').split(',')
            for t in tenants_to_delete:
                if t in existing_tenants:
                    existing_tenants.remove(t)
            existing_tenants = ','.join(existing_tenants) if existing_tenants \
                else ''
            _LOG.debug(f'Removing {self.attribute_name} attribute')
            self.delete_attribute(target_user)
            self.update_attribute(target_user, existing_tenants)
            return build_response(
                code=HTTPStatus.OK,
                content=f'Attribute {self.attribute_name} for user '
                        f'{target_user} has been updated.')

        if not self.get_attribute_value(target_user):
            _LOG.debug(
                f'Attribute {self.attribute_name} for user {target_user} '
                f'does not exist')
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=f'Attribute {self.attribute_name} for user '
                        f'{target_user} does not exist')

        _LOG.debug(f'Removing {self.attribute_name} attribute')
        self.delete_attribute(target_user)
        return build_response(
            code=HTTPStatus.OK,
            content=f'Attribute {self.attribute_name} for user '
                    f'{target_user} has been deleted.')
