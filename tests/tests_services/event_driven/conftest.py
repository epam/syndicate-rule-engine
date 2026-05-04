"""Shared fixtures and sample payloads for event_driven tests."""

from __future__ import annotations

import pytest

from helpers.constants import Cloud
from services.event_driven.domain.constants import (
    EB_CLOUDTRAIL_API_CALL_DETAIL_TYPE,
    EB_DETAIL,
    EB_DETAIL_TYPE,
    MA_CLOUD,
    MA_EVENT_METADATA,
    MA_EVENT_NAME,
    MA_EVENT_SOURCE,
    MA_REGION_NAME,
    MA_TENANT_NAME,
)


@pytest.fixture
def sample_eventbridge_cloudtrail_event() -> dict:
    return {
        EB_DETAIL_TYPE: EB_CLOUDTRAIL_API_CALL_DETAIL_TYPE,
        EB_DETAIL: {
            "awsRegion": "us-east-1",
            "eventSource": "ec2.amazonaws.com",
            "eventName": "RunInstances",
            "userIdentity": {"accountId": "123456789012"},
        },
    }


@pytest.fixture
def sample_maestro_event() -> dict:
    return {
        MA_TENANT_NAME: "tenant-alpha",
        MA_REGION_NAME: "us-west-2",
        MA_EVENT_METADATA: {
            MA_CLOUD: Cloud.AWS.value,
            MA_EVENT_SOURCE: "maestro.source",
            MA_EVENT_NAME: "MaestroEvent",
        },
    }
