from __future__ import annotations

from helpers.constants import Cloud
from services.event_driven.domain.models import EventRecord
from services.event_driven.utils import digest_, without_duplicates


def test_digest_stable_for_same_logical_record():
    a = EventRecord(
        cloud=Cloud.AWS,
        region_name="r",
        source_name="s",
        event_name="e",
        account_id="1",
        tenant_name=None,
    ).to_event_record_attribute()
    b = EventRecord(
        cloud=Cloud.AWS,
        region_name="r",
        source_name="s",
        event_name="e",
        account_id="1",
        tenant_name=None,
    ).to_event_record_attribute()
    assert digest_(a) == digest_(b)


def test_without_duplicates_drops_second_identical():
    rec = EventRecord(
        cloud=Cloud.AWS,
        region_name="r",
        source_name="s",
        event_name="e",
        account_id="1",
        tenant_name=None,
    ).to_event_record_attribute()
    out = list(without_duplicates([rec, rec]))
    assert len(out) == 1


def test_without_duplicates_keeps_distinct():
    first = EventRecord(
        cloud=Cloud.AWS,
        region_name="r",
        source_name="s",
        event_name="e1",
        account_id="1",
        tenant_name=None,
    ).to_event_record_attribute()
    second = EventRecord(
        cloud=Cloud.AWS,
        region_name="r",
        source_name="s",
        event_name="e2",
        account_id="1",
        tenant_name=None,
    ).to_event_record_attribute()
    out = list(without_duplicates([first, second]))
    assert len(out) == 2
