from __future__ import annotations

from helpers import deep_get
from helpers.constants import AWS_VENDOR, Cloud
from services.event_driven.adapters.base import BaseEventAdapter
from services.event_driven.domain import (
    CT_ACCOUNT_ID,
    CT_EVENT_NAME,
    CT_EVENT_SOURCE,
    CT_REGION,
    CT_USER_IDENTITY,
    EB_DETAIL,
    EventRecord,
)


class EventBridgeEventAdapter(BaseEventAdapter):
    def __init__(self):
        super().__init__(vendor=AWS_VENDOR)

    def to_event_record(self, event: dict) -> EventRecord:
        return EventRecord(
            cloud=Cloud.AWS,
            region_name=deep_get(event, (EB_DETAIL, CT_REGION)),
            source_name=deep_get(event, (EB_DETAIL, CT_EVENT_SOURCE)),
            event_name=deep_get(event, (EB_DETAIL, CT_EVENT_NAME)),
            account_id=deep_get(event, (EB_DETAIL, CT_USER_IDENTITY, CT_ACCOUNT_ID)),
            tenant_name=None,
        )
