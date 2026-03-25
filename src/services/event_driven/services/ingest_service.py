"""
Ingest events: adapt, filter by rules, save.
Single place for parsing and rules filtering.
"""

from __future__ import annotations

from dataclasses import dataclass

from helpers.constants import AWS_VENDOR, MAESTRO_VENDOR
from helpers import batches
from helpers.log_helper import get_logger
from models.event import EventRecordAttribute
from services.environment_service import EnvironmentService
from services.event_driven.adapters import EventRecordsAdapter
from services.event_driven.services.event_store_service import (
    EventStoreService,
)
from services.event_driven.utils import without_duplicates

from .rules_service import EventDrivenRulesService

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
        ed_rules_service: EventDrivenRulesService,
    ):
        self._event_store_service = event_store_service
        self._environment_service = environment_service
        self._ed_rules_service = ed_rules_service

    def ingest(
        self,
        raw_events: list[dict],
        vendor: str | None = None,
    ) -> IngestResult:
        """
        Adapt, filter by rules, save. Single parse pass.
        vendor: if set, use for all events; else auto-detect per event (AWS, Maestro).
        """
        events_in_item = (
            self._environment_service.number_of_native_events_in_event_item()
        )
        received = len(raw_events)
        by_vendor: dict[str, list[EventRecordAttribute]] = {}

        for raw in raw_events:
            if vendor is not None:
                adapter = EventRecordsAdapter(vendor, [raw])
                attr = adapter.adapt_single(raw)
                v = vendor
            else:
                detected = self._detect_vendor_and_adapt(raw)
                if detected is None:
                    _LOG.debug("Could not detect vendor for event %s", raw)
                    continue
                v, attr = detected

            if attr is None:
                _LOG.debug("Could not adapt event %s", raw)
                continue
            if not self._ed_rules_service.get_rules(attr):
                _LOG.debug("Event %s does not match any rules", attr)
                continue
            by_vendor.setdefault(v, []).append(attr)

        total_saved = 0
        batch_jobs: list[tuple[str, list[EventRecordAttribute]]] = []
        for v, adapted in by_vendor.items():
            adapted = list(without_duplicates(adapted))
            total_saved += len(adapted)
            for batch in batches(adapted, events_in_item):
                batch_jobs.append((v, batch))

        if batch_jobs:
            entities = (
                self._event_store_service.create(events=batch, vendor=v)
                for v, batch in batch_jobs
            )
            self._event_store_service.batch_save(entities)

        return IngestResult(
            received=received,
            saved=total_saved,
            rejected=received - total_saved,
        )

    def _detect_vendor_and_adapt(
        self,
        raw: dict,
    ) -> tuple[str, EventRecordAttribute] | None:
        """Try AWS then Maestro; return (vendor, event_attr) or None."""
        for v in (AWS_VENDOR, MAESTRO_VENDOR):
            adapter = EventRecordsAdapter(v, [raw])
            attr = adapter.adapt_single(raw)
            if attr is not None:
                return (v, attr)
        _LOG.debug("Could not detect vendor for event %s", raw)
        return None
