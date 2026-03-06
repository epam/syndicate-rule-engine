from http import HTTPStatus
import logging

from helpers import batches
from helpers.constants import Endpoint, HTTPMethod
from helpers.lambda_response import build_response
from helpers.log_helper import get_logger
from handlers import AbstractHandler, Mapping
from services import SP
from services.environment_service import EnvironmentService
from services.event_driven import EventProcessorService, EventService, get_adapter
from validators.swagger_request_models import EventPostModel
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)


class EventsHandler(AbstractHandler):
    """
    Handles events ingestion from external systems.

    Description:
        - Validates and converts event records to EventRecordAttribute.
        - Adapts event records to EventRecordAttribute.
        - Saves event records to the database.
    """

    def __init__(
        self,
        events_service: EventService,
        event_processor_service: EventProcessorService,
        environment_service: EnvironmentService,
    ):
        self._es = events_service
        self._eps = event_processor_service
        self._env = environment_service

    @classmethod
    def build(cls) -> "EventsHandler":
        return cls(
            events_service=SP.event_service,
            event_processor_service=SP.event_processor_service,
            environment_service=SP.environment_service,
        )

    @property
    def mapping(self) -> Mapping:
        return {Endpoint.EVENT: {HTTPMethod.POST: self.event_action}}

    @validate_kwargs
    def event_action(self, event: EventPostModel):
        events_in_item = self._env.number_of_native_events_in_event_item()

        _LOG.info("Initializing event processors")
        processor = self._eps.get_processor(event.vendor)
        processor.events = event.events
        n_received = processor.number_of_received()
        adapter = get_adapter(event.vendor)
        adapted_events, failed_events = adapter.adapt(
            processor.without_duplicates(processor.prepared_events())
        )

        if failed_events:
            _LOG.warning(
                "Failed to adapt %s events for vendor %s. "
                "These records are skipped from persistence and logged for debug.",
                len(failed_events),
                event.vendor,
            )
            if _LOG.isEnabledFor(logging.DEBUG):
                _LOG.debug(
                    "Rejected events payload for vendor %s: %s",
                    event.vendor,
                    [
                        {
                            "error": failed_event.error,
                            "event": failed_event.event,
                        }
                        for failed_event in failed_events
                    ],
                )

        entities = (
            self._es.create(events=batch, vendor=event.vendor)
            for batch in batches(adapted_events, events_in_item)
        )
        if adapted_events:
            self._es.batch_save(entities)

        return build_response(
            code=HTTPStatus.ACCEPTED,
            content={"received": n_received, "saved": len(adapted_events)},
        )
