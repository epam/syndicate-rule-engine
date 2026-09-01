from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from helpers.constants import Cloud
from models.event import Event
from services.event_driven.domain.models import EventRecord
from services.event_driven.services.event_store_service import EventStoreService


def test_create_sets_partition_timestamp_vendor():
    env = MagicMock()
    env.events_ttl_hours.return_value = 0
    env.number_of_partitions_for_events.return_value = 10

    fixed = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    rec = EventRecord(
        cloud=Cloud.AWS,
        region_name="r",
        source_name="s",
        event_name="e",
        account_id="1",
        tenant_name=None,
    ).to_event_record_attribute()

    with patch(
        "services.event_driven.services.event_store_service.random.randrange",
        return_value=7,
    ):
        with patch(
            "services.event_driven.services.event_store_service.utc_datetime",
            return_value=fixed,
        ):
            svc = EventStoreService(env)
            model = svc.create(events=[rec], vendor="AWS")

    assert model.partition == 7
    assert model.vendor == "AWS"
    assert model.timestamp == fixed.timestamp()
    assert model.ttl is None


def test_batch_save_iterates_models():
    m1, m2 = MagicMock(), MagicMock()
    mock_writer = MagicMock()
    mock_cm = MagicMock()
    mock_cm.__enter__.return_value = mock_writer
    mock_cm.__exit__.return_value = None

    with patch.object(Event, "batch_write", return_value=mock_cm):
        EventStoreService.batch_save([m1, m2])

    mock_writer.save.assert_any_call(m1)
    mock_writer.save.assert_any_call(m2)
    assert mock_writer.save.call_count == 2


def test_get_events_builds_range_condition():
    env = MagicMock()
    env.event_assembler_pull_item_limit.return_value = 50
    mock_query = MagicMock(return_value=[])
    with patch.object(Event, "query", mock_query):
        svc = EventStoreService(env)
        list(svc.get_events(partition=3, since=1.0, till=2.0))

    mock_query.assert_called_once()
    kwargs = mock_query.call_args.kwargs
    assert kwargs["hash_key"] == 3
    assert kwargs["page_size"] == 50
