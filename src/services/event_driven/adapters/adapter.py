from __future__ import annotations

from typing import Any, Callable, Dict, List

from pydantic import ValidationError

from helpers.constants import AWS_VENDOR, MAESTRO_VENDOR
from helpers.log_helper import get_logger
from models.event import EventRecordAttribute
from services.event_driven.adapters.base import BaseEventAdapter
from services.event_driven.adapters.event_bridge import EventBridgeEventAdapter
from services.event_driven.adapters.maestro import MaestroEventAdapter

from ..domain import FailedEvent

_LOG = get_logger(__name__)


class EventRecordsAdapter:
    """
    Adapter for event records.
    """

    _ADAPTERS: dict[str, Callable[[], BaseEventAdapter]] = {
        AWS_VENDOR: EventBridgeEventAdapter,
        MAESTRO_VENDOR: MaestroEventAdapter,
    }

    def __init__(self, vendor: str, events: List[Dict[str, Any]]) -> None:
        self._events = events
        self._vendor = vendor
        self._adapter = self._ADAPTERS[vendor]()

    @property
    def events(self) -> List[Dict[str, Any]]:
        return self._events

    def number_of_received(self) -> int:
        return len(self._events)

    def adapt_single(
        self,
        raw_event: Dict[str, Any],
    ) -> EventRecordAttribute | None:
        """
        Adapt a single raw event to EventRecordAttribute.
        Returns None if adaptation fails (ValidationError, ValueError, TypeError).
        """
        try:
            record = self._adapter.to_event_record(raw_event)
            return record.to_event_record_attribute()
        except (ValidationError, ValueError, TypeError):
            return None

    def adapt(self) -> tuple[list[EventRecordAttribute], list[FailedEvent]]:
        return self._adapter.adapt(self._events)
