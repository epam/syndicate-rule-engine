from __future__ import annotations

from dataclasses import dataclass

from helpers import batches
from helpers.log_helper import get_logger
from services.environment_service import EnvironmentService
from services.event_driven.adapters import EventRecordsAdapter
from services.event_driven.services.event_store_service import (
    EventStoreService,
)
from services.event_driven.utils import without_duplicates

_LOG = get_logger(__name__)


@dataclass(frozen=True)
class IngestResult:
    received: int
    saved: int
    rejected: int


class EventIngestService:
    def __init__(
        self,
        event_store_service: EventStoreService,
        environment_service: EnvironmentService,
    ):
        self._event_store_service = event_store_service
        self._environment_service = environment_service

    def ingest(self, vendor: str, events: list[dict]) -> IngestResult:
        events_in_item = (
            self._environment_service.number_of_native_events_in_event_item()
        )
        adapter = EventRecordsAdapter(vendor, events)
        adapted_events, failed_events = adapter.adapt()
        adapted_events = list(without_duplicates(adapted_events))

        if failed_events:
            _LOG.warning(
                "Failed to adapt %s events for vendor %s. "
                "These records are skipped from persistence and logged for debug.",
                len(failed_events),
                vendor,
            )
            _LOG.debug(
                "Rejected events payload for vendor %s: %s",
                vendor,
                [
                    {
                        "error": failed_event.error,
                        "event": failed_event.event,
                    }
                    for failed_event in failed_events
                ],
            )

        entities = (
            self._event_store_service.create(events=batch, vendor=vendor)
            for batch in batches(adapted_events, events_in_item)
        )
        if adapted_events:
            self._event_store_service.batch_save(entities)

        return IngestResult(
            received=adapter.number_of_received(),
            saved=len(adapted_events),
            rejected=len(failed_events),
        )
