from __future__ import annotations

from helpers import deep_get
from helpers.constants import MAESTRO_VENDOR, Cloud
from services.event_driven.adapters.base import BaseEventAdapter
from services.event_driven.domain import (
    MA_CLOUD,
    MA_EVENT_METADATA,
    MA_EVENT_NAME,
    MA_EVENT_SOURCE,
    MA_REGION_NAME,
    MA_REQUEST,
    MA_TENANT_NAME,
    EventRecord,
)


class MaestroEventAdapter(BaseEventAdapter):
    def __init__(self):
        super().__init__(vendor=MAESTRO_VENDOR)

    @staticmethod
    def _resolve_cloud(event: dict) -> Cloud:
        cloud_value = deep_get(event, (MA_EVENT_METADATA, MA_REQUEST, MA_CLOUD))
        if cloud_value is None:
            cloud_value = deep_get(event, (MA_EVENT_METADATA, MA_CLOUD))
        cloud = Cloud.parse(cloud_value) if isinstance(cloud_value, str) else None
        if cloud is None:
            raise ValueError(f"Unsupported cloud value: {cloud_value}")
        return cloud

    def to_event_record(self, event: dict) -> EventRecord:
        return EventRecord(
            cloud=self._resolve_cloud(event),
            region_name=deep_get(event, (MA_REGION_NAME,)),
            source_name=deep_get(event, (MA_EVENT_METADATA, MA_EVENT_SOURCE)),
            event_name=deep_get(event, (MA_EVENT_METADATA, MA_EVENT_NAME)),
            account_id=None,
            tenant_name=deep_get(event, (MA_TENANT_NAME,)),
        )
