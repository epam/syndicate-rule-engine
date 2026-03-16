from __future__ import annotations

import random
from datetime import timedelta
from typing import Iterable, Optional, cast

from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime
from models.event import Event, EventRecordAttribute
from services.environment_service import EnvironmentService

_LOG = get_logger(__name__)


class EventStoreService:
    def __init__(self, environment_service: EnvironmentService) -> None:
        self._environment_service = environment_service

    def create(self, events: list[EventRecordAttribute], vendor: str) -> Event:
        ttl_hours, ttl = self._environment_service.events_ttl_hours(), None
        now = utc_datetime()
        if ttl_hours:
            ttl = int((now + timedelta(hours=ttl_hours)).timestamp())
        partition = random.randrange(
            self._environment_service.number_of_partitions_for_events()
        )
        return Event(
            partition=partition,
            timestamp=now.timestamp(),
            events=events,
            vendor=vendor,
            ttl=ttl,
        )

    def get_events(
        self,
        partition: int,
        since: Optional[float] = None,
        till: Optional[float] = None,
    ) -> Iterable[Event]:
        page_size = self._environment_service.event_assembler_pull_item_limit()
        range_key_condition = None
        if since is not None and till is not None:
            range_key_condition = Event.timestamp.between(since, till)
        elif since is not None:
            range_key_condition = Event.timestamp > since  # type: ignore[assignment]
        elif till is not None:
            range_key_condition = Event.timestamp <= till  # type: ignore[assignment]
        _LOG.debug(
            f"Querying partition {partition} with range_key_condition: {range_key_condition}"
        )
        return cast(
            Iterable[Event],
            Event.query(
                hash_key=partition,
                range_key_condition=range_key_condition,
                scan_index_forward=True,
                page_size=page_size,
            ),
        )

    @classmethod
    def save(cls, event: Event) -> bool:
        event.save()
        return True

    @classmethod
    def delete(cls, event: Event) -> bool:
        event.delete()
        return True

    @classmethod
    def batch_save(cls, events: Iterable[Event]) -> None:
        with Event.batch_write() as writer:
            for model in events:
                writer.save(model)

    @classmethod
    def batch_delete(cls, events: Iterable[Event]) -> None:
        with Event.batch_write() as writer:
            for model in events:
                writer.delete(model)
