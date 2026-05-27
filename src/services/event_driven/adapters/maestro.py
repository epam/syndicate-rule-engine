from __future__ import annotations

from helpers import deep_get
from helpers.constants import MAESTRO_VENDOR
from services.event_driven.adapters.base import BaseEventAdapter
from services.event_driven.domain import (
    MA_CLOUD,
    MA_EVENT_METADATA,
    MA_EVENT_NAME,
    MA_EVENT_SOURCE,
    MA_REGION_NAME,
    MA_TENANT_NAME,
    EventRecord,
)


class MaestroEventAdapter(BaseEventAdapter):
    def __init__(self):
        super().__init__(vendor=MAESTRO_VENDOR)

    def to_event_record(self, event: dict) -> EventRecord:
        return EventRecord(
            cloud=deep_get(event, (MA_EVENT_METADATA, MA_CLOUD)),
            region_name=deep_get(event, (MA_REGION_NAME,)),
            source_name=deep_get(event, (MA_EVENT_METADATA, MA_EVENT_SOURCE)),
            event_name=deep_get(event, (MA_EVENT_METADATA, MA_EVENT_NAME)),
            account_id=None,
            tenant_name=deep_get(event, (MA_TENANT_NAME,)),
        )
