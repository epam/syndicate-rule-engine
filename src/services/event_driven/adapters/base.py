from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Iterable

from pydantic import ValidationError

from models.event import EventRecordAttribute
from services.event_driven.domain import EventRecordUnion, FailedEvent


class BaseEventAdapter(ABC):
    def __init__(self, vendor: str):
        self.vendor = vendor

    @abstractmethod
    def to_event_record(self, event: dict[str, Any]) -> EventRecordUnion:
        raise NotImplementedError

    def adapt(
        self, events: Iterable[dict[str, Any]]
    ) -> tuple[list[EventRecordAttribute], list[FailedEvent]]:
        valid_events: list[EventRecordAttribute] = []
        failed_events: list[FailedEvent] = []
        for event in events:
            try:
                record = self.to_event_record(event)
                valid_events.append(record.to_event_record_attribute())
            except (ValidationError, ValueError, TypeError) as e:
                failed_events.append(
                    FailedEvent(
                        vendor=self.vendor,
                        event=event,
                        error=str(e),
                    )
                )
        return valid_events, failed_events
