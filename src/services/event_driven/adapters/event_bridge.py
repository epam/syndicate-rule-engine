from __future__ import annotations

from helpers import deep_get
from helpers.constants import AWS_VENDOR, Cloud
from services.event_driven.adapters.base import BaseEventAdapter
from services.event_driven.domain import (
    AwsEventRecord,
    AwsMetadata,
    CT_ACCOUNT_ID,
    CT_EVENT_NAME,
    CT_EVENT_SOURCE,
    CT_REGION,
    CT_REQUEST_PARAMETERS,
    CT_RESPONSE_ELEMENTS,
    CT_USER_IDENTITY,
    EB_CLOUDTRAIL_API_CALL_DETAIL_TYPE,
    EB_DETAIL,
    EB_DETAIL_TYPE,
)


_REQUEST_PARAMETERS_LOC = (EB_DETAIL, CT_REQUEST_PARAMETERS)
_RESPONSE_ELEMENTS_LOC = (EB_DETAIL, CT_RESPONSE_ELEMENTS)
_REGION_NAME_LOC = (EB_DETAIL, CT_REGION)
_EVENT_SOURCE_LOC = (EB_DETAIL, CT_EVENT_SOURCE)
_EVENT_NAME_LOC = (EB_DETAIL, CT_EVENT_NAME)
_ACCOUNT_ID_LOC = (EB_DETAIL, CT_USER_IDENTITY, CT_ACCOUNT_ID)


class EventBridgeEventAdapter(BaseEventAdapter):
    def __init__(self):
        super().__init__(vendor=AWS_VENDOR)

    def to_event_record(self, event: dict) -> AwsEventRecord:
        event_type = deep_get(event, (EB_DETAIL_TYPE,))
        if event_type != EB_CLOUDTRAIL_API_CALL_DETAIL_TYPE:
            raise ValueError(
                f'Expected {EB_DETAIL_TYPE} to be '
                f'{EB_CLOUDTRAIL_API_CALL_DETAIL_TYPE!r}, but got {event_type!r}'
            )
        request_parameters = deep_get(event, _REQUEST_PARAMETERS_LOC)
        response_elements = deep_get(event, _RESPONSE_ELEMENTS_LOC)
        return AwsEventRecord(
            cloud=Cloud.AWS,
            region_name=deep_get(event, _REGION_NAME_LOC),
            source_name=deep_get(event, _EVENT_SOURCE_LOC),
            event_name=deep_get(event, _EVENT_NAME_LOC),
            account_id=deep_get(event, _ACCOUNT_ID_LOC),
            tenant_name=None,
            metadata=AwsMetadata(
                requestParameters=request_parameters,
                responseElements=response_elements,
            ),
        )
