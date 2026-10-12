from __future__ import annotations

import pytest
from pydantic import ValidationError

from helpers.constants import Cloud
from models.event import EventRecordAttribute
from services.event_driven.domain.models import EventRecord


def test_event_record_rejects_missing_tenant_identifiers():
    with pytest.raises(ValidationError):
        EventRecord(
            cloud=Cloud.AWS,
            region_name="us-east-1",
            source_name="x",
            event_name="y",
            account_id=None,
            tenant_name=None,
        )


def test_event_record_accepts_account_id_only():
    rec = EventRecord(
        cloud=Cloud.AWS,
        region_name="us-east-1",
        source_name="ec2.amazonaws.com",
        event_name="RunInstances",
        account_id="123456789012",
        tenant_name=None,
    )
    attr = rec.to_event_record_attribute()
    assert isinstance(attr, EventRecordAttribute)
    assert attr.cloud == Cloud.AWS.value
    assert attr.account_id == "123456789012"
    assert attr.tenant_name is None


def test_event_record_accepts_tenant_name_only():
    rec = EventRecord(
        cloud=Cloud.AWS,
        region_name="eu-west-1",
        source_name="s",
        event_name="e",
        account_id=None,
        tenant_name="acme",
    )
    attr = rec.to_event_record_attribute()
    assert attr.tenant_name == "acme"
    assert attr.account_id is None
