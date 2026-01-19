from http import HTTPStatus
from typing import MutableMapping

from typing_extensions import Self

from handlers.event_assembler_handler import (
    EventAssemblerHandler,
    EventRemoverHandler,
)
from helpers import RequestContext
from helpers.constants import ACTION_PARAM
from helpers.lambda_response import LambdaOutput, build_response
from helpers.log_helper import get_logger
from services.abs_lambda import EventProcessorLambdaHandler


_LOG = get_logger(__name__)

CLEAR_EVENTS_ACTION = "clear-events"
ASSEMBLE_EVENTS_ACTION = "assemble-events"

Handler = EventRemoverHandler | EventAssemblerHandler


class EventHandler(EventProcessorLambdaHandler):
    processors = ()

    def __init__(self, handlers: dict[str, Handler]) -> None:
        self._handlers = handlers

    @classmethod
    def build(cls) -> Self:
        return cls(
            handlers={
                CLEAR_EVENTS_ACTION: EventRemoverHandler.instantiate(),
                ASSEMBLE_EVENTS_ACTION: EventAssemblerHandler.instantiate(),
            }
        )

    def handle_request(
        self,
        event: MutableMapping,
        context: RequestContext,
    ) -> LambdaOutput:
        event_action = event.get(ACTION_PARAM) or ASSEMBLE_EVENTS_ACTION
        _LOG.info(f"Event action: `{event_action}`. Retrieving handler")

        handler = self._handlers.get(event_action)
        if not handler:
            message = (
                f"Not available action: {event_action}. "
                f"Available: {', '.join(self._handlers.keys())}"
            )
            _LOG.warning(message)
            return build_response(code=HTTPStatus.BAD_REQUEST, content=message)

        return handler.handler(event)


HANDLER = EventHandler.build()


def lambda_handler(event: dict, context: RequestContext) -> LambdaOutput:
    return HANDLER.lambda_handler(event=event, context=context)
