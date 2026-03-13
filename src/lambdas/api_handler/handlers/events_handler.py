from http import HTTPStatus

from typing_extensions import Self

from handlers import AbstractHandler, Mapping
from helpers.constants import Endpoint, HTTPMethod
from helpers.lambda_response import build_response
from helpers.log_helper import get_logger
from services import SP
from services.event_driven import EventIngestService, EventDrivenRulesService
from services.event_driven.adapters import EventRecordsAdapter
from validators.swagger_request_models import EventPostModel
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)


class EventsHandler(AbstractHandler):
    """Thin API handler over EventIngestService with event filtering by rules."""

    def __init__(
        self,
        event_ingest_service: EventIngestService,
        ed_rules_service: EventDrivenRulesService,
    ):
        self._event_ingest_service = event_ingest_service
        self._ed_rules_service = ed_rules_service

    @classmethod
    def build(cls) -> Self:
        return cls(
            event_ingest_service=SP.event_ingest_service,
            ed_rules_service=SP.ed_rules_service,
        )

    @property
    def mapping(self) -> Mapping:
        return {Endpoint.EVENT: {HTTPMethod.POST: self.event_action}}

    @validate_kwargs
    def event_action(self, event: EventPostModel):
        _LOG.info("Starting event ingestion")
        vendor = event.vendor
        events = event.events
        adapter = EventRecordsAdapter(vendor=vendor, events=events)

        processable_raw: list[dict] = []
        filtered_count = 0

        for raw_event in events:
            event_attr = adapter.adapt_single(raw_event)
            if event_attr is None:
                filtered_count += 1
                continue
            rules = self._ed_rules_service.get_rules(event_attr)
            if rules:
                processable_raw.append(raw_event)
            else:
                filtered_count += 1

        result = self._event_ingest_service.ingest(
            vendor=vendor, events=processable_raw
        )

        return build_response(
            code=HTTPStatus.ACCEPTED,
            content={
                "received": len(events),
                "saved": result.saved,
            },
        )
