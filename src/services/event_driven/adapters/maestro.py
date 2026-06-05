from __future__ import annotations

from helpers import deep_get
from helpers.constants import MAESTRO_VENDOR, Cloud
from services.event_driven.adapters.base import BaseEventAdapter
from services.event_driven.domain import (
    AwsEventRecord,
    AwsMetadata,
    MA_CLOUD,
    MA_AWS_REQUEST_PARAMETERS,
    MA_AWS_RESPONSE_ELEMENTS,
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

    def to_event_record(self, event: dict) -> EventRecord | AwsEventRecord:
        cloud = deep_get(event, (MA_EVENT_METADATA, MA_CLOUD))
        if cloud == Cloud.AWS.value:
            request_parameters = deep_get(
                event, (MA_EVENT_METADATA, MA_AWS_REQUEST_PARAMETERS)
            )
            response_elements = deep_get(
                event, (MA_EVENT_METADATA, MA_AWS_RESPONSE_ELEMENTS)
            )
            return AwsEventRecord(
                cloud=Cloud.AWS,
                region_name=deep_get(event, (MA_REGION_NAME,)),
                source_name=deep_get(
                    event, (MA_EVENT_METADATA, MA_EVENT_SOURCE)
                ),
                event_name=deep_get(event, (MA_EVENT_METADATA, MA_EVENT_NAME)),
                account_id=None,
                tenant_name=deep_get(event, (MA_TENANT_NAME,)),
                metadata=AwsMetadata(
                    requestParameters=request_parameters,
                    responseElements=response_elements,
                ),
            )
        return EventRecord(
            cloud=cloud,
            region_name=deep_get(event, (MA_REGION_NAME,)),
            source_name=deep_get(event, (MA_EVENT_METADATA, MA_EVENT_SOURCE)),
            event_name=deep_get(event, (MA_EVENT_METADATA, MA_EVENT_NAME)),
            account_id=None,
            tenant_name=deep_get(event, (MA_TENANT_NAME,)),
        )
