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

    def get_events(self, partition: int, since: Optional[float] = None,
                   till: Optional[float] = None) -> Iterable[Event]:
        """
        since < Iterable[Event] <= till
        :param partition:
        :param since:
        :param till:
        :return:
        """
        limit = self._environment_service.event_assembler_pull_item_limit()
        rkc = None
        if since and till:
            rkc = Event.timestamp.between(since, till)
        elif since:
            rkc = (Event.timestamp > since)
        elif till:
            rkc = (Event.timestamp <= till)
        cursor = Event.query(hash_key=partition,
                             range_key_condition=rkc,
                             scan_index_forward=True,
                             limit=limit)
        events = list(cursor)
        lek = cursor.last_evaluated_key
        while lek:
            _LOG.info(f'Going to query for {limit} events more.')
            cursor = Event.query(
                hash_key=partition,
                range_key_condition=rkc,
                scan_index_forward=True,
                limit=limit,
                last_evaluated_key=lek,
            )
            events.extend(cursor)
            lek = cursor.last_evaluated_key
        return events

    @staticmethod
    def get_dto(entity: Event):
        _json = entity.get_json()
        _json.pop('partition', None)
        _json.pop('ttl', None)
        return _json

    @classmethod
    def save(cls, event: Event):
        try:
            event.save()
            return True
        except (Exception, BaseException) as e:
            _LOG.warning(f'{event} could not be persisted, due to: {e}.')
        return False

    @classmethod
    def delete(cls, event: Event):
        try:
            event.delete()
            return True
        except (Exception, BaseException) as e:
            _LOG.warning(f'{event} could not be removed, due to: {e}.')
        return False

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
