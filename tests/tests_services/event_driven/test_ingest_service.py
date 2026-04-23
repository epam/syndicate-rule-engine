from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from helpers.constants import AWS_VENDOR, MAESTRO_VENDOR
from services.event_driven.services.ingest_service import EventIngestService


@pytest.fixture
def environment_service() -> MagicMock:
    env = MagicMock()
    env.number_of_native_events_in_event_item.return_value = 100
    env.number_of_partitions_for_events.return_value = 4
    env.events_ttl_hours.return_value = 0
    return env


@pytest.fixture
def event_store_service() -> MagicMock:
    store = MagicMock()
    store.create.return_value = MagicMock(name="EventModel")

    def consume_entities(entities):
        list(entities)

    store.batch_save.side_effect = consume_entities
    return store


@pytest.fixture
def ed_rules_service() -> MagicMock:
    return MagicMock()


@pytest.fixture
def ingest_service(
    event_store_service: MagicMock,
    environment_service: MagicMock,
    ed_rules_service: MagicMock,
) -> EventIngestService:
    return EventIngestService(
        event_store_service=event_store_service,
        environment_service=environment_service,
        ed_rules_service=ed_rules_service,
    )


def test_ingest_saves_when_rules_match(
    ingest_service: EventIngestService,
    event_store_service: MagicMock,
    ed_rules_service: MagicMock,
    sample_eventbridge_cloudtrail_event,
):
    ed_rules_service.get_rules.return_value = {"r1"}

    result = ingest_service.ingest(
        [sample_eventbridge_cloudtrail_event],
        vendor=AWS_VENDOR,
    )

    assert result.received == 1
    assert result.saved == 1
    assert result.rejected == 0
    event_store_service.create.assert_called_once()
    event_store_service.batch_save.assert_called_once()


def test_ingest_skips_when_no_rules(
    ingest_service: EventIngestService,
    event_store_service: MagicMock,
    ed_rules_service: MagicMock,
    sample_eventbridge_cloudtrail_event,
):
    ed_rules_service.get_rules.return_value = None

    result = ingest_service.ingest(
        [sample_eventbridge_cloudtrail_event],
        vendor=AWS_VENDOR,
    )

    assert result.saved == 0
    assert result.rejected == result.received
    event_store_service.batch_save.assert_not_called()


def test_ingest_auto_detect_maestro_vendor(
    ingest_service: EventIngestService,
    event_store_service: MagicMock,
    ed_rules_service: MagicMock,
    sample_maestro_event,
):
    ed_rules_service.get_rules.return_value = {"r1"}

    result = ingest_service.ingest([sample_maestro_event], vendor=None)

    assert result.saved == 1
    call_kw = event_store_service.create.call_args.kwargs
    assert call_kw["vendor"] == MAESTRO_VENDOR


def test_ingest_fixed_vendor_mismatch_drops_event(
    ingest_service: EventIngestService,
    event_store_service: MagicMock,
    ed_rules_service: MagicMock,
    sample_maestro_event,
):
    ed_rules_service.get_rules.return_value = {"r1"}

    result = ingest_service.ingest([sample_maestro_event], vendor=AWS_VENDOR)

    assert result.saved == 0
    event_store_service.batch_save.assert_not_called()


def test_ingest_empty_raw_events(
    ingest_service: EventIngestService,
    event_store_service: MagicMock,
):
    result = ingest_service.ingest([], vendor=AWS_VENDOR)

    assert result.received == 0
    assert result.saved == 0
    assert result.rejected == 0
    event_store_service.batch_save.assert_not_called()


def test_ingest_skips_when_get_rules_returns_empty_set(
    ingest_service: EventIngestService,
    event_store_service: MagicMock,
    ed_rules_service: MagicMock,
    sample_eventbridge_cloudtrail_event,
):
    ed_rules_service.get_rules.return_value = set()

    result = ingest_service.ingest(
        [sample_eventbridge_cloudtrail_event],
        vendor=AWS_VENDOR,
    )

    assert result.saved == 0
    assert result.rejected == result.received
    event_store_service.batch_save.assert_not_called()


def test_ingest_auto_detect_aws_when_vendor_none(
    ingest_service: EventIngestService,
    event_store_service: MagicMock,
    ed_rules_service: MagicMock,
    sample_eventbridge_cloudtrail_event,
):
    ed_rules_service.get_rules.return_value = {"r1"}

    result = ingest_service.ingest(
        [sample_eventbridge_cloudtrail_event],
        vendor=None,
    )

    assert result.saved == 1
    assert event_store_service.create.call_args.kwargs["vendor"] == AWS_VENDOR


def test_ingest_vendor_none_unparseable_raw_skipped(
    ingest_service: EventIngestService,
    event_store_service: MagicMock,
    ed_rules_service: MagicMock,
):
    ed_rules_service.get_rules.return_value = {"r1"}

    result = ingest_service.ingest([{}], vendor=None)

    assert result.received == 1
    assert result.saved == 0
    assert result.rejected == 1
    event_store_service.batch_save.assert_not_called()
    ed_rules_service.get_rules.assert_not_called()


def test_ingest_fixed_vendor_adapt_fails_skipped(
    ingest_service: EventIngestService,
    event_store_service: MagicMock,
    ed_rules_service: MagicMock,
    sample_maestro_event,
):
    """Maestro-shaped payload with AWS adapter → adapt_single returns None."""
    ed_rules_service.get_rules.return_value = {"r1"}

    result = ingest_service.ingest([sample_maestro_event], vendor=AWS_VENDOR)

    assert result.received == 1
    assert result.saved == 0
    assert result.rejected == 1
    ed_rules_service.get_rules.assert_not_called()


def test_ingest_multiple_events_same_vendor_one_batch(
    ingest_service: EventIngestService,
    event_store_service: MagicMock,
    ed_rules_service: MagicMock,
    sample_eventbridge_cloudtrail_event,
):
    ed_rules_service.get_rules.return_value = {"r1"}
    second = {
        **sample_eventbridge_cloudtrail_event,
        "detail": {
            **sample_eventbridge_cloudtrail_event["detail"],
            "eventName": "StopInstances",
        },
    }

    result = ingest_service.ingest(
        [sample_eventbridge_cloudtrail_event, second],
        vendor=AWS_VENDOR,
    )

    assert result.received == 2
    assert result.saved == 2
    assert result.rejected == 0
    assert event_store_service.create.call_count == 1
    event_store_service.batch_save.assert_called_once()


def test_ingest_multiple_vendors_single_batch_save(
    ingest_service: EventIngestService,
    event_store_service: MagicMock,
    ed_rules_service: MagicMock,
    sample_eventbridge_cloudtrail_event,
    sample_maestro_event,
):
    ed_rules_service.get_rules.return_value = {"r1"}

    result = ingest_service.ingest(
        [sample_eventbridge_cloudtrail_event, sample_maestro_event],
        vendor=None,
    )

    assert result.received == 2
    assert result.saved == 2
    assert result.rejected == 0
    event_store_service.batch_save.assert_called_once()
    assert event_store_service.create.call_count == 2
    vendors_saved = {
        event_store_service.create.call_args_list[i].kwargs["vendor"] for i in range(2)
    }
    assert vendors_saved == {AWS_VENDOR, MAESTRO_VENDOR}


def test_ingest_mixed_accept_reject_counts(
    ingest_service: EventIngestService,
    event_store_service: MagicMock,
    ed_rules_service: MagicMock,
    sample_eventbridge_cloudtrail_event,
):
    def get_rules_side_effect(attr):
        if attr.event_name == "RunInstances":
            return {"r1"}
        return None

    ed_rules_service.get_rules.side_effect = get_rules_side_effect
    second = {
        **sample_eventbridge_cloudtrail_event,
        "detail": {
            **sample_eventbridge_cloudtrail_event["detail"],
            "eventName": "StopInstances",
        },
    }

    result = ingest_service.ingest(
        [sample_eventbridge_cloudtrail_event, second, {}],
        vendor=AWS_VENDOR,
    )

    assert result.received == 3
    assert result.saved == 1
    assert result.rejected == 2
    event_store_service.batch_save.assert_called_once()


def test_ingest_deduplicates_identical_events_before_save(
    ingest_service: EventIngestService,
    event_store_service: MagicMock,
    ed_rules_service: MagicMock,
    sample_eventbridge_cloudtrail_event,
):
    ed_rules_service.get_rules.return_value = {"r1"}

    result = ingest_service.ingest(
        [sample_eventbridge_cloudtrail_event, sample_eventbridge_cloudtrail_event],
        vendor=AWS_VENDOR,
    )

    assert result.received == 2
    assert result.saved == 1
    assert result.rejected == 1
    event_store_service.create.assert_called_once()


def test_ingest_splits_into_multiple_create_calls_when_batch_size_one(
    ingest_service: EventIngestService,
    environment_service: MagicMock,
    event_store_service: MagicMock,
    ed_rules_service: MagicMock,
    sample_eventbridge_cloudtrail_event,
):
    environment_service.number_of_native_events_in_event_item.return_value = 1
    ed_rules_service.get_rules.return_value = {"r1"}
    second = {
        **sample_eventbridge_cloudtrail_event,
        "detail": {
            **sample_eventbridge_cloudtrail_event["detail"],
            "eventName": "StopInstances",
        },
    }

    result = ingest_service.ingest(
        [sample_eventbridge_cloudtrail_event, second],
        vendor=AWS_VENDOR,
    )

    assert result.saved == 2
    assert event_store_service.create.call_count == 2
    event_store_service.batch_save.assert_called_once()
