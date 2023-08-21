from abc import ABC, abstractmethod

from helpers.log_helper import get_logger
from models.scheduled_job import ScheduledJob
from services.clients.event_bridge import EventBridgeClient

_LOG = get_logger(__name__)

TARGET_ID = 'custodian-batch-job-target'

RATE_EXPRESSION_UNITS = {
    'minute', 'minutes', 'hour', 'hours', 'day', 'days', 'week', 'weeks',
    'second', 'seconds',
}


class AbstractJobScheduler(ABC):

    @abstractmethod
    def update_job(self, item: ScheduledJob, is_enabled: bool):
        """
        Updates the data of registered job
        """


class APJobScheduler(AbstractJobScheduler):
    def __init__(self):
        self._scheduler = None

    @property
    def scheduler(self):
        """Not needed here"""
        raise NotImplementedError()

    def update_job(self, item: ScheduledJob, is_enabled: bool):
        _id = item.id
        _LOG.info(f'Updating scheduled job with id \'{_id}\'')
        item.update_with(is_enabled=is_enabled)
        # hack because job's target function is situated not in docker but
        # in Custodian itself. We cannot pause it using "scheduler.pause_job"
        if not is_enabled:
            item._additional_data['next_run_time'] = None
        else:
            _LOG.warning('You are simply not supposed to use this '
                         'functional in docker.')
            # item._additional_data['next_run_time'] = time() + ...
        item.save()
        _LOG.info(
            f'Scheduled job with name \'{_id}\' was successfully updated'
        )

        return item


class EventBridgeJobScheduler(AbstractJobScheduler):

    def __init__(self, client: EventBridgeClient):
        self._client = client

    def update_job(self, item: ScheduledJob, is_enabled: bool):
        _id = item.id
        enabling_rule_map = {
            True: self._client.enable_rule,
            False: self._client.disable_rule
        }
        _LOG.info(f'Updating scheduled job with id \'{_id}\'')
        params = dict(rule_name=_id)
        if enabling_rule_map.get(is_enabled)(**params):
            _LOG.info(f'Rule`s status was changed to \'{is_enabled}\'')
        else:
            return None

        item.update_with(is_enabled=is_enabled)
        item.save()
        _LOG.debug('Scheduled job`s data was saved to Dynamodb')
        _LOG.info(
            f'Scheduled job with name \'{_id}\' was successfully updated'
        )
        return item
