import random
from datetime import timedelta
from typing import Optional, Iterable

from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime
from models.event import Event
from services.environment_service import EnvironmentService

_LOG = get_logger(__name__)


class EventService:
    def __init__(self, environment_service: EnvironmentService):
        self._environment_service = environment_service

    def create(self, events: list, vendor: str) -> Event:
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
            ttl=ttl
        )

    def get_events(
        self,
        partition: int,
        since: Optional[float] = None,
        till: Optional[float] = None
    ) -> Iterable[Event]:
        """
        since < Iterable[Event] <= till
        :param partition:
        :param since:
        :param till:
        :return:
        """
        page_size = self._environment_service.event_assembler_pull_item_limit()
        rkc = None
        if since is not None and till is not None:
            rkc = Event.timestamp.between(since, till)
        elif since is not None:
            rkc = Event.timestamp > since
        elif till is not None:
            rkc = Event.timestamp <= till
        _LOG.debug(f"Querying partition {partition} with range_key_condition: {rkc}")
        return Event.query(
            hash_key=partition,
            range_key_condition=rkc,
            scan_index_forward=True,
            page_size=page_size
        )

    @classmethod
    def save(cls, event: Event):
        event.save()
        return True

    @classmethod
    def delete(cls, event: Event):
        event.delete()
        return True

    @classmethod
    def batch_save(cls, events: Iterable[Event]):
        with Event.batch_write() as writer:
            for model in events:
                writer.save(model)

    @classmethod
    def batch_delete(cls, events: Iterable[Event]):
        with Event.batch_write() as writer:
            for model in events:
                writer.delete(model)
