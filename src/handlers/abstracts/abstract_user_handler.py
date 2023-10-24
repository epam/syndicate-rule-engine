from abc import abstractmethod
from http import HTTPStatus

from handlers.abstracts.abstract_handler import AbstractHandler
from helpers import validate_params, build_response
from helpers.log_helper import get_logger

_LOG = get_logger(__name__)


class AbstractUserHandler(AbstractHandler):
    @abstractmethod
    def get_attribute_value(self, *args, **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def check_user_exist(self, *args, **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def update_attribute(self, *args, **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def validate_value(self, *args, **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def delete_attribute(self, *args, **kwargs):
        raise NotImplementedError()

    @property
    @abstractmethod
    def attribute_name(self):
        raise NotImplementedError()

    def _basic_get_user_attribute_handler(self, event: dict):
        _LOG.debug(f'Get {self.attribute_name} attribute for user: {event}')
        validate_params(event=event, required_params_list=['user_id'])

        target_user = event.get('target_user')
        if not target_user:
            target_user = event.get('user_id')

        self._validate_user_existence(
            username=target_user)

        _LOG.debug(f'Receiving {self.attribute_name} attribute')
        attribute_value = self.get_attribute_value(target_user)

        return build_response(
            code=HTTPStatus.OK,
            content={self.attribute_name: attribute_value})

    def _basic_set_user_attribute_handler(self, event: dict):
        _LOG.debug(f'Create {self.attribute_name} attribute for user: {event}')
        validate_params(event=event, required_params_list=['user_id'])

        target_user = event.get('target_user')
        if not target_user:
            target_user = event.get('user_id')

        self._validate_user_existence(
            username=target_user)

        if self.get_attribute_value(target_user):
            _LOG.error(f'Attribute {self.attribute_name} for user '
                       f'{target_user} already exists')
            return build_response(
                code=HTTPStatus.CONFLICT,
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
        self.validate_value(event=event)

        _LOG.debug(f'Creating {self.attribute_name} attribute')
        self.update_attribute(target_user, attribute_value)

        return build_response(
            code=HTTPStatus.CREATED,
            content={self.attribute_name: attribute_value})

    def _basic_update_user_attribute_handler(self, event: dict):
        _LOG.debug(f'Update {self.attribute_name} attribute for user: '
                   f'{event}')
        validate_params(event=event, required_params_list=['user_id'])

        target_user = event.get('target_user')
        if not target_user:
            target_user = event.get('user_id')

        self._validate_user_existence(
            username=target_user)

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

        self.validate_value(event=event)

        _LOG.debug(f'Updating {self.attribute_name} attribute')
        self.update_attribute(target_user, attribute_value)

        return build_response(
            code=HTTPStatus.OK,
            content={self.attribute_name: attribute_value})

    def _basic_delete_user_attribute_handler(self, event: dict):
        _LOG.debug(f'Delete {self.attribute_name} attribute for user: {event}')
        validate_params(event=event, required_params_list=['user_id'])

        target_user = event.get('target_user')
        if not target_user:
            target_user = event.get('user_id')

        self._validate_user_existence(
            username=target_user)

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
            content=f'Attribute {self.attribute_name} for user '
                    f'{target_user} has been deleted.')

    def _validate_user_existence(self, username: str):
        if not self.check_user_exist(username):
            _LOG.debug(f'{username} does not exist')
            return build_response(
                code=HTTPStatus.NOT_FOUND,
                content=f'{username} does not exist')

    def _validate_target_user_attr_presence(self, target_user):
        if not target_user:
            _LOG.error('Target user is missing')
            return build_response(
                code=HTTPStatus.BAD_REQUEST,
                content=f'Can not update attribute {self.attribute_name}: '
                        f'target user is missing.')
