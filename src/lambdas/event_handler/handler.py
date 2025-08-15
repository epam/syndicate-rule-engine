from functools import cached_property
from http import HTTPStatus

from handlers.event_assembler_handler import (EventAssemblerHandler,
                                              EventRemoverHandler)
from helpers import RequestContext
from helpers.constants import ACTION_PARAM
from helpers.lambda_response import build_response
from helpers.log_helper import get_logger
from services.abs_lambda import EventProcessorLambdaHandler

_LOG = get_logger(__name__)

CLEAR_EVENTS_ACTION = 'clear-events'
ASSEMBLE_EVENTS_ACTION = 'assemble-events'

Handler = EventRemoverHandler | EventAssemblerHandler


class EventHandler(EventProcessorLambdaHandler):
    processors = ()

    def __init__(self):
        self._action_handler: dict[str, Handler] = {}

    def handle_request(self, event: dict, context: RequestContext):
        _default_action = ASSEMBLE_EVENTS_ACTION
        event_action = event.get(ACTION_PARAM) or _default_action
        _LOG.info(f'Event action: `{event_action}`. Retrieving handler')
        handler = self.get_handler(event_action)
        if not handler:
            message = \
                f'Not available action: {event_action}. ' \
                f'Available: {", ".join(self.action_handler_map.keys())}'
            _LOG.warning(message)
            return build_response(code=HTTPStatus.BAD_REQUEST,
                                  content=message)
        return handler.handler(event)

    def get_handler(self, action: str) -> Handler | None:
        if action not in self._action_handler:
            _LOG.info(f'Instantiating handler for {action} action')
            _type = self.action_handler_map.get(action)
            if _type:
                self._action_handler[action] = _type.instantiate()
        return self._action_handler.get(action)

    @cached_property
    def action_handler_map(self) -> dict[str, type[Handler]]:
        return {
            CLEAR_EVENTS_ACTION: EventRemoverHandler,
            ASSEMBLE_EVENTS_ACTION: EventAssemblerHandler
        }


HANDLER = EventHandler()


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
