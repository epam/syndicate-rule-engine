import abc
from http import HTTPStatus
from typing import Callable, Dict, Optional, Any

from helpers import build_response
from helpers.log_helper import get_logger

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
            return build_response(code=HTTPStatus.BAD_REQUEST,
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
            return build_response(code=HTTPStatus.BAD_REQUEST,
                                  content=str(e))
        return entity

    def _assert_exists(self, entity: Optional[Any] = None,
                       message: str = None, **kwargs) -> None:
        if not entity:
            _message = (message or ENTITY_DOES_NOT_EXIST_MESSAGE).format(
                **kwargs)
            _LOG.info(_message)
            return build_response(code=HTTPStatus.NOT_FOUND,
                                  content=_message)

    def _assert_does_not_exist(self, entity: Optional[Any] = None,
                               message: str = None, **kwargs) -> None:
        if entity:
            _message = (message or ENTITY_ALREADY_EXISTS_MESSAGE).format(
                **kwargs)
            _LOG.info(_message)
            return build_response(code=HTTPStatus.CONFLICT,
                                  content=_message)


class AbstractComposedHandler(AbstractHandler):

    def __init__(self, resource_map: Dict[str, Dict[str, AbstractHandler]]):
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
