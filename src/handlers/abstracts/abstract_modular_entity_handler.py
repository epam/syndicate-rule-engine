from abc import abstractmethod
from http import HTTPStatus
from typing import Iterable, Callable, Dict, Union, Type, List, Optional

from modular_sdk.models.pynamodb_extension.base_model import LastEvaluatedKey

from handlers.abstracts.abstract_handler import AbstractHandler
from helpers import build_response
from helpers.constants import NEXT_TOKEN_ATTR
from helpers.log_helper import get_logger
from services.modular_service import ModularService

_LOG = get_logger(__name__)

UNRESOLVABLE_ERROR = 'Request has run into an issue, which could not' \
                     ' be resolved.'

BEGUN_TEMPLATE = '{action} has begun: {event}.'
FAILED_TEMPLATE = '{action} has failed.'
SUSPENDED_TEMPLATE = '{action} has suspended the execution.'
RESPONSE_TEMPLATE = '{action} is going to respond with {code}' \
                    ' status-code and "{content}" content.'

ENTITY_TEMPLATE = '{entity}:\'{id}\''


class AbstractModularEntityHandler(AbstractHandler):
    """
    Provides abstract behaviour of Maestro Common Domain Model Entities.
    """

    _code: Optional[int]
    _content: Optional[str]
    _meta: Optional[dict]

    def __init__(self, modular_service: ModularService):
        self.modular_service = modular_service
        self._reset()

    def _reset(self):
        # Denoting response variables.
        self._code = None
        self._content = None
        self._meta = None

    @property
    def response(self):
        """
        Delegated to dispatch a response of a pending request, based on
        a running instance response-mandating variables.
        Given absence of assigned values, default internal, unresolvable
        errors are to dispatch.
        :raises: ApplicationException
        :return: Dict
        """
        _code = self._code or HTTPStatus.INTERNAL_SERVER_ERROR
        _content = UNRESOLVABLE_ERROR if self._content is None else \
            self._content
        _meta = self._meta or {}
        self._reset()
        return build_response(code=_code, content=_content, meta=_meta)

    @property
    @abstractmethod
    def entity(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def responsibilities(self) -> Dict[
        str, Iterable[Callable[[Dict], Union[Dict, Type[None]]]]
    ]:
        """
        Returns a dictionary object, maintaining names of responsibilities
        which reference iterable sequences of callable items, each of
        which connote concern-segregated step of a flow.
        :return: Iterable[Iterable]
        """
        raise NotImplementedError

    @abstractmethod
    def _produce_response_dto(self, event: Optional[Dict] = None) -> \
            Union[str, Dict, List, Type[None]]:
        """
        Mandates derivation of a query-response data transfer object.
        :parameter event: Optional[Dict]
        :return: Union[Dict, List, Type[None]]
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def attributes_to_log(self) -> Dict[Union[str, Type[None]], Iterable[str]]:
        """
        Returns attribute names which are meant to be logged during a
        particular concern-flow.
        Note: denoting a None key, explicitly stresses a default subject list.
        :return: Dict[Union[str, Type[None]], Iterable[str]]
        """
        raise NotImplementedError

    def last_evaluated_key(self) -> LastEvaluatedKey:
        """Returns last evaluated key instance which contains key from a
        request the class is responsible for,
        if the next key actually exists"""
        return LastEvaluatedKey()  # empty

    def _process_action(self, event: Dict, action: str):
        """
        Base action processor, which provides sequential execution
        of each `step` in respective chain-alike flow.
        Given any aforementioned step has failed to produce an event
        for the following one to feed on - the execution halts.
        :raises: CustodianException
        :return: Dict[str, str]
        """
        action = f'{action} {self.entity.capitalize()}'
        _event_input = ','.join(f'\'{k}\' = {v}' for k, v in event.items())

        _LOG.info(BEGUN_TEMPLATE.format(action=action, event=_event_input))

        # Steps through each concreate responsibility chain
        for name, flow in self.responsibilities.items():

            if not event:
                break

            for index, step in enumerate(flow, 1):
                # Merges all request-bound attributes
                _vars = {**self.__dict__, **event}

                # Prepares an attribute-list to log for a step
                to_log: dict = self.attributes_to_log
                log_key = name if name in self.attributes_to_log else None
                attrs: Iterable = to_log.get(log_key, list(_vars))

                _action = f'{name.capitalize()}:{index})'

                _i_input = (f'\'{k}\' = {_vars.get(k)}' for k in attrs)
                _i_step = ', '.join(_i_input)

                _LOG.info(BEGUN_TEMPLATE.format(action=_action, event=_i_step))

                event = step(event)
                if not event:
                    _LOG.warning(SUSPENDED_TEMPLATE.format(action=_action))
                    break

        if event:
            dto = self._produce_response_dto(event=event)

            self._code = HTTPStatus.OK if dto is not None else self._code
            self._content = dto if dto is not None else self._content
            _lek = self.last_evaluated_key()
            self._meta = {
                NEXT_TOKEN_ATTR: _lek.serialize()} if _lek else None

        _log = dict(action=action, content=self._content, code=self._code)
        _LOG.info(RESPONSE_TEMPLATE.format(**_log))

        return self.response
