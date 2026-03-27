from http import HTTPStatus

from typing_extensions import Self

from handlers import AbstractHandler, Mapping
from helpers.constants import Endpoint, HTTPMethod
from helpers.lambda_response import build_response
from helpers.log_helper import get_logger
from services import SP
from services.event_driven import EventIngestService
from validators.swagger_request_models import EventPostModel
from validators.utils import validate_kwargs

_LOG = get_logger(__name__)


class EventsHandler(AbstractHandler):
    def __init__(self, event_ingest_service: EventIngestService):
        self._event_ingest_service = event_ingest_service

    @classmethod
    def build(cls) -> Self:
        return cls(event_ingest_service=SP.event_ingest_service)

    @property
    def mapping(self) -> Mapping:
        return {Endpoint.EVENT: {HTTPMethod.POST: self.event_action}}

    @validate_kwargs
    def event_action(self, event: EventPostModel):
        _LOG.info("Starting event ingestion")
        result = self._event_ingest_service.ingest(
            raw_events=event.events,
            vendor=event.vendor,
        )
        return build_response(
            code=HTTPStatus.ACCEPTED,
            content={
                "received": result.received,
                "saved": result.saved,
            },
        )
