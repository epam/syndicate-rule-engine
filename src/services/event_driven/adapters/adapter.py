from __future__ import annotations

from typing import Any, Callable, Dict, List

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
        if not isinstance(events, list):
            raise ValueError(f"Events must be a list, got {type(events)}")
        self._events = events
        self._vendor = vendor
        self._adapter = self._ADAPTERS[vendor]()

    @property
    def events(self) -> List[Dict[str, Any]]:
        return self._events

    def number_of_received(self) -> int:
        return len(self._events)

    def adapt(self) -> tuple[list[EventRecordAttribute], list[FailedEvent]]:
        return self._adapter.adapt(self._events)
