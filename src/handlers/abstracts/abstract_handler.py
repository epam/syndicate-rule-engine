import abc
from typing import Callable, Dict, Optional, Any

from helpers import build_response, RESPONSE_RESOURCE_NOT_FOUND_CODE, \
    RESPONSE_BAD_REQUEST_CODE, RESPONSE_CONFLICT
from helpers.log_helper import get_logger
from helpers.constants import AWS_CLOUD_ATTR, AZURE_CLOUD_ATTR, GCP_CLOUD_ATTR


CLOUDS = (AWS_CLOUD_ATTR, AZURE_CLOUD_ATTR, GCP_CLOUD_ATTR)

_LOG = get_logger(__name__)

ENTITY_DOES_NOT_EXIST_MESSAGE = 'The requested entity was not found'
ENTITY_ALREADY_EXISTS_MESSAGE = 'The requested entity already exists'


class AbstractHandler:

    @abc.abstractmethod
    def define_action_mapping(self):
        """
        Should return dict of the following format:
        {
            ACTION_NAME: {
                METHOD: self.handler_func
            }
        }
        """
        raise NotImplementedError()

    @staticmethod
    def _validate_type(var_name, var_value, required_type, required=True):
        if required:
            error = not isinstance(var_value, required_type)
        else:
            error = var_value and not isinstance(
                var_value, required_type)

        if error:
            _LOG.debug(f'{var_name} must be a valid '
                       f'{required_type.__name__}')
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content=f'\'{var_name}\' must be a {required_type.__name__}')

    @staticmethod
    def _replace_dict_params(event,
                             full_params_list: list,
                             default_params_mapping: dict):
        """Replace all parameters that can be default and then get from
        event all parameters"""

        entity_data = {}
        for param in full_params_list:
            if param not in event and param in default_params_mapping:
                entity_data[param] = default_params_mapping.get(param)
            else:
                param_value = event.get(param)
                if param_value is not None:
                    entity_data[param] = param_value
        return entity_data

    @staticmethod
    def _save_entity(entity_data: dict,
                     entity_name: str,
                     create_func: Callable,
                     save_func: Callable,
                     get_dto_func: Callable):
        try:
            entity = create_func(entity_data)
            _LOG.debug(f'{entity_name} configuration object json: '
                       f'{entity.get_json()}')
            save_func(entity)
        except (ValueError, TypeError) as e:
            return build_response(code=RESPONSE_BAD_REQUEST_CODE,
                                  content=str(e))
        return get_dto_func(entity)

    @staticmethod
    def _replace_obj_params(event: dict,
                            entity: object,
                            full_params_list: list,
                            entity_name: str,
                            save_func: Callable):
        """
        For each parameter in DB model:
            1. Get it from event
            2. Check if different from existing
            3. Set attribute to new
            4. Then save this attribute to DB
        """

        try:
            for attr in full_params_list:
                attr_value = event.get(attr)
                if attr_value is not None \
                        and attr_value != getattr(entity, attr):
                    _LOG.debug(f'Setting {attr} attribute to {attr_value} '
                               f'for {entity_name}')
                    setattr(entity, attr, attr_value)

            save_func(entity)
        except (TypeError, ValueError) as e:
            return build_response(code=RESPONSE_BAD_REQUEST_CODE,
                                  content=str(e))
        return entity

    @staticmethod
    def _validate_cloud(cloud: str):
        if cloud.upper() not in CLOUDS:
            _LOG.debug(f'Unsupported cloud: {cloud}. '
                       f'Available clouds: {CLOUDS}')
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content=f'Unsupported cloud: {cloud}. '
                        f'Available clouds: {CLOUDS}')

    def _assert_exists(self, entity: Optional[Any] = None,
                       message: str = None, **kwargs) -> None:
        if not entity:
            _message = (message or ENTITY_DOES_NOT_EXIST_MESSAGE).format(**kwargs)
            _LOG.info(_message)
            return build_response(code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                                  content=_message)

    def _assert_does_not_exist(self, entity: Optional[Any] = None,
                               message: str = None, **kwargs) -> None:
        if entity:
            _message = (message or ENTITY_ALREADY_EXISTS_MESSAGE).format(**kwargs)
            _LOG.info(_message)
            return build_response(code=RESPONSE_CONFLICT,
                                  content=_message)


class AbstractComposedHandler(AbstractHandler):

    def __init__(
        self, resource_map: Dict[str, Dict[str, AbstractHandler]]
    ):
        self._action_mapping = resource_map

    def define_action_mapping(self):
        return {
            resource: {
                method: handler.define_action_mapping().
                get(resource, dict()).get(method)
                for method, handler in method_map.items()
            }
            for resource, method_map in self._action_mapping.items()
        }

    def define_handler_mapping(self) -> Dict[str, Dict[str, AbstractHandler]]:
        return self._action_mapping

