from __future__ import annotations

from helpers.constants import AWS_VENDOR, MAESTRO_VENDOR
from services.event_driven.adapters.adapter import EventRecordsAdapter
from services.event_driven.domain.constants import EB_DETAIL_TYPE


def test_adapt_batch_empty():
    adapter = EventRecordsAdapter(AWS_VENDOR, [])
    ok, failed = adapter.adapt()
    assert ok == []
    assert failed == []


def test_adapt_batch_all_valid_aws(sample_eventbridge_cloudtrail_event):
    adapter = EventRecordsAdapter(AWS_VENDOR, [sample_eventbridge_cloudtrail_event])
    ok, failed = adapter.adapt()
    assert len(ok) == 1
    assert failed == []
    assert ok[0].event_name == "RunInstances"


def test_adapt_batch_all_invalid_aws(sample_eventbridge_cloudtrail_event):
    bad = {**sample_eventbridge_cloudtrail_event, EB_DETAIL_TYPE: "Wrong"}
    adapter = EventRecordsAdapter(AWS_VENDOR, [bad])
    ok, failed = adapter.adapt()
    assert ok == []
    assert len(failed) == 1
    assert failed[0].vendor == AWS_VENDOR
    assert failed[0].event is bad
    assert "Wrong" in failed[0].error or "Expected" in failed[0].error


def test_adapt_batch_mixed_valid_and_invalid(
    sample_eventbridge_cloudtrail_event,
):
    bad = {**sample_eventbridge_cloudtrail_event, EB_DETAIL_TYPE: "Wrong"}
    adapter = EventRecordsAdapter(
        AWS_VENDOR,
        [sample_eventbridge_cloudtrail_event, bad],
    )
    ok, failed = adapter.adapt()
    assert len(ok) == 1
    assert len(failed) == 1


def test_adapt_batch_maestro_valid_and_invalid(sample_maestro_event):
    adapter = EventRecordsAdapter(
        MAESTRO_VENDOR,
        [sample_maestro_event, {}],
    )
    ok, failed = adapter.adapt()
    assert len(ok) == 1
    assert len(failed) == 1
    assert failed[0].vendor == MAESTRO_VENDOR
    assert failed[0].event == {}


def test_adapt_single_aws_eventbridge_ok(sample_eventbridge_cloudtrail_event):
    adapter = EventRecordsAdapter(AWS_VENDOR, [sample_eventbridge_cloudtrail_event])
    attr = adapter.adapt_single(sample_eventbridge_cloudtrail_event)
    assert attr is not None
    assert attr.source_name == "ec2.amazonaws.com"
    assert attr.event_name == "RunInstances"
    assert attr.account_id == "123456789012"


def test_adapt_single_aws_invalid_detail_type_returns_none(
    sample_eventbridge_cloudtrail_event,
):
    raw = {**sample_eventbridge_cloudtrail_event, EB_DETAIL_TYPE: "OtherType"}
    adapter = EventRecordsAdapter(AWS_VENDOR, [raw])
    assert adapter.adapt_single(raw) is None


def test_adapt_single_maestro_ok(sample_maestro_event):
    adapter = EventRecordsAdapter(MAESTRO_VENDOR, [sample_maestro_event])
    attr = adapter.adapt_single(sample_maestro_event)
    assert attr is not None
    assert attr.tenant_name == "tenant-alpha"
    assert attr.source_name == "maestro.source"
    assert attr.event_name == "MaestroEvent"
    assert attr.account_id is None


def test_adapt_single_maestro_missing_fields_returns_none():
    adapter = EventRecordsAdapter(MAESTRO_VENDOR, [{}])
    assert adapter.adapt_single({}) is None
