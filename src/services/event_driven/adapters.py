from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Iterable

from helpers import deep_get
from helpers.constants import AWS_VENDOR, MAESTRO_VENDOR, Cloud
from models.event import EventRecordAttribute
from pydantic import ValidationError

from ._constants import (
    CT_ACCOUNT_ID,
    CT_EVENT_NAME,
    CT_EVENT_SOURCE,
    CT_REGION,
    CT_USER_IDENTITY,
    EB_DETAIL,
    MA_CLOUD,
    MA_EVENT_METADATA,
    MA_EVENT_NAME,
    MA_EVENT_SOURCE,
    MA_REGION_NAME,
    MA_REQUEST,
    MA_TENANT_NAME,
)
from ._utils import EventRecord


@dataclass(frozen=True)
class FailedEvent:
    vendor: str
    event: dict[str, Any]
    error: str


class BaseEventAdapter(ABC):
    def __init__(self, vendor: str):
        self.vendor = vendor

    @abstractmethod
    def to_event_record(self, event: dict[str, Any]) -> EventRecord:
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


class EventBridgeEventAdapter(BaseEventAdapter):
    def __init__(self):
        super().__init__(vendor=AWS_VENDOR)

    def to_event_record(self, event: dict[str, Any]) -> EventRecord:
        return EventRecord(
            cloud=Cloud.AWS,
            region_name=deep_get(event, (EB_DETAIL, CT_REGION)),
            source_name=deep_get(event, (EB_DETAIL, CT_EVENT_SOURCE)),
            event_name=deep_get(event, (EB_DETAIL, CT_EVENT_NAME)),
            account_id=deep_get(event, (EB_DETAIL, CT_USER_IDENTITY, CT_ACCOUNT_ID)),
            tenant_name=None,
        )


class MaestroEventAdapter(BaseEventAdapter):
    def __init__(self):
        super().__init__(vendor=MAESTRO_VENDOR)

    @staticmethod
    def _resolve_cloud(event: dict[str, Any]) -> Cloud:
        cloud_value = deep_get(event, (MA_EVENT_METADATA, MA_REQUEST, MA_CLOUD))
        if cloud_value is None:
            cloud_value = deep_get(event, (MA_EVENT_METADATA, MA_CLOUD))
        cloud = Cloud.parse(cloud_value) if isinstance(cloud_value, str) else None
        if cloud is None:
            raise ValueError(f'Unsupported cloud value: {cloud_value}')
        return cloud

    def to_event_record(self, event: dict[str, Any]) -> EventRecord:
        return EventRecord(
            cloud=self._resolve_cloud(event),
            region_name=deep_get(event, (MA_REGION_NAME,)),
            source_name=deep_get(event, (MA_EVENT_METADATA, MA_EVENT_SOURCE)),
            event_name=deep_get(event, (MA_EVENT_METADATA, MA_EVENT_NAME)),
            account_id=None,
            tenant_name=deep_get(event, (MA_TENANT_NAME,)),
        )


def get_adapter(vendor: str) -> BaseEventAdapter:
    mapping: dict[str, Callable[[], BaseEventAdapter]] = {
        AWS_VENDOR: EventBridgeEventAdapter,
        MAESTRO_VENDOR: MaestroEventAdapter,
    }
    try:
        return mapping[vendor]()
    except KeyError as e:
        raise ValueError(f'Unsupported vendor: {vendor}') from e
