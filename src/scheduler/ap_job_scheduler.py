import os
from functools import cached_property
from http import HTTPStatus
from typing import Optional

from apscheduler.jobstores.base import ConflictingIdError, JobLookupError
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.base import BaseTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.util import datetime_to_utc_timestamp
from bson.binary import Binary
from modular_sdk.models.tenant import Tenant
from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError

from helpers.constants import CAASEnv, CUSTODIAN_TYPE, SCHEDULED_JOB_TYPE, \
    BatchJobEnv
from helpers.lambda_response import ResponseFactory
from helpers.log_helper import get_logger
from helpers.time_helper import utc_iso
from models.scheduled_job import SJ_ID_ATTR, SJ_CREATION_DATE_ATTR, \
    SJ_TYPE_ATTR, SJ_CONTEXT_ATTR, SJ_LAST_EXECUTION_TIME_ATTR
from models.scheduled_job import ScheduledJob, SCHEDULED_JOBS_TABLE_NAME
from services.clients.batch import SubprocessBatchClient
from services.clients.scheduler import AbstractJobScheduler
from services.setting_service import SettingsService

try:
    import cPickle as pickle
except ImportError:  # pragma: nocover
    import pickle

_LOG = get_logger(__name__)

RATE_EXPRESSION_UNITS = {
    'minute', 'minutes', 'hour', 'hours', 'day', 'days', 'week', 'weeks',
    'second', 'seconds',
}
RETRY_CRON_NAME_PATTERN = 'report_retry_'


class CustomMongoDBJobStore(MongoDBJobStore):
    """
    Custom representation of APScheduler's MongoDBJobStore. It brings no
    new logic, just changes the structure of the document which is saved to
    MongoDB according to the Custodian's & Modular requirements.
    The base class has the following structure:
    {
        _id: $job_id,
        next_run_time: $timestamp,
        job_state: Binary($job_obj_pickled)
    }
    Current structure:
    {
        _id: ObjectId("uuid"),  # native MongoDB's id,
        id: $job_id,
        next_run_time: $timestamp,
        creation_date: $iso_format_utc,
        type: "CUSTODIAN:SCHEDULED_JOB",  # const
        context: {
            job_state: Binary($job_obj_pickled),
            ...
        }
    }
    Some other attributes are added to the document afterwards.
    """
    collection: Collection

    def lookup_job(self, job_id):
        document = self.collection.find_one({SJ_ID_ATTR: job_id},
                                            ['context.job_state'])
        return self._reconstitute_job(
            document.get(SJ_CONTEXT_ATTR, {})['job_state']
        ) if document else None

    def add_job(self, job):
        try:
            self.collection.insert_one({
                SJ_ID_ATTR: job.id,
                'next_run_time': datetime_to_utc_timestamp(job.next_run_time),
                SJ_TYPE_ATTR: f'{CUSTODIAN_TYPE}:{SCHEDULED_JOB_TYPE}',
                SJ_CREATION_DATE_ATTR: utc_iso(),
                SJ_CONTEXT_ATTR: {
                    'job_state': Binary(pickle.dumps(job.__getstate__(),
                                                     self.pickle_protocol))
                }
            })
        except DuplicateKeyError:
            raise ConflictingIdError(job.id)

    def update_job(self, job):
        changes = {
            'next_run_time': datetime_to_utc_timestamp(job.next_run_time),
            f'{SJ_CONTEXT_ATTR}.job_state': Binary(
                pickle.dumps(job.__getstate__(), self.pickle_protocol)),
            SJ_LAST_EXECUTION_TIME_ATTR: utc_iso()
        }
        result = self.collection.update_one({SJ_ID_ATTR: job.id},
                                            {'$set': changes})
        if result and result.matched_count == 0:
            raise JobLookupError(job.id)

    def remove_job(self, job_id):
        result = self.collection.delete_one({SJ_ID_ATTR: job_id})
        if result and result.deleted_count == 0:
            raise JobLookupError(job_id)

    def _get_jobs(self, conditions):
        jobs = []
        failed_job_ids = []
        for document in self.collection.find(
                conditions, [SJ_ID_ATTR, f'{SJ_CONTEXT_ATTR}.job_state'],
                sort=[('next_run_time', ASCENDING)]):
            try:
                jobs.append(self._reconstitute_job(
                    document.get(SJ_CONTEXT_ATTR, {})['job_state']))
            except BaseException:
                self._logger.exception('Unable to restore job "%s" -- '
                                       'removing it', document[SJ_ID_ATTR])
                failed_job_ids.append(document[SJ_ID_ATTR])

        # Remove all the jobs we failed to restore
        if failed_job_ids:
            self.collection.delete_many({SJ_ID_ATTR: {'$in': failed_job_ids}})

        return jobs


class APJobScheduler(AbstractJobScheduler):
    def __init__(self, batch_client: SubprocessBatchClient,
                 setting_service: SettingsService):
        self._batch_client = batch_client
        self.setting_service = setting_service

    @cached_property
    def scheduler(self) -> BackgroundScheduler:
        scheduler = BackgroundScheduler()
        scheduler.configure(jobstores={
            'default': CustomMongoDBJobStore(
                database=os.getenv(CAASEnv.MONGO_DATABASE),
                collection=SCHEDULED_JOBS_TABLE_NAME,
                client=MongoClient(os.getenv(CAASEnv.MONGO_URI))
            )  # or BaseModel.mongodb_handler().mongodb.client
        })
        return scheduler

    def start(self):
        self.scheduler.start()

    @staticmethod
    def _valid_cron(schedule: str) -> CronTrigger:
        """
        May raise ValueError
        """
        return CronTrigger.from_crontab(
            schedule.strip().replace('cron', '').strip(' ()')
        )

    @staticmethod
    def _valid_interval(schedule: str) -> IntervalTrigger:
        """
        May raise ValueError
        """
        # validated beforehand so must be valid
        value, unit = schedule.strip().replace('rate', '').strip(' ()').split()
        value = int(value)
        if not unit.endswith('s'):
            unit += 's'
        return IntervalTrigger(**{unit: value})

    def derive_trigger(self, schedule: str) -> BaseTrigger:
        """
        First valid is returned
        """
        _validators = [self._valid_cron, self._valid_interval]
        error = None
        for v in _validators:
            try:
                return v(schedule)
            except ValueError as e:
                error = e
        if error:
            _LOG.warning(f'User has sent invalid schedule '
                         f'expression: \'{schedule}\'')
            raise ResponseFactory(HTTPStatus.BAD_REQUEST).errors([{
                'location': ['schedule'],
                'description': f'Invalid schedule expression: {error}'
            }]).exc()

    def register_job(self, tenant: Tenant, schedule: str,
                     environment: dict,
                     name: Optional[str] = None,
                     rulesets: list[str] | None = None) -> ScheduledJob:
        _id = self.safe_name(name) if name else \
            self.safe_name_from_tenant(tenant)
        environment[BatchJobEnv.SCHEDULED_JOB_NAME.value] = _id
        _LOG.info(f'Registering new scheduled job with id \'{_id}\'')
        self.scheduler.add_job(
            func=self._batch_client.submit_job,
            id=_id,
            trigger=self.derive_trigger(schedule),
            kwargs=dict(
                environment_variables=environment,
                job_name=_id,  # not so important
            )
        )
        # here it's not a mistake. self.scheduler.add_job above creates an
        # item in MongoDB, so we just query it
        _job = ScheduledJob.get_nullable(hash_key=_id)
        _LOG.info('Updating the created job item with some '
                  'Custodian`s required attributes')
        actions = [
            ScheduledJob.tenant_name.set(tenant.name),
            ScheduledJob.customer_name.set(tenant.customer_name),
            ScheduledJob.context['schedule'].set(schedule),
            ScheduledJob.context['scan_regions'].set(
                self._scan_regions_from_env(environment)),
            ScheduledJob.context['scan_rulesets'].set(rulesets),
            ScheduledJob.context['is_enabled'].set(True)
        ]
        _job.update(actions=actions)
        _LOG.info(f'Scheduled job with name \'{_id}\' was added')
        return _job

    def deregister_job(self, _id: str):
        _LOG.info(f'Removing the job with id \'{_id}\'')
        _LOG.debug('Removing the job from APScheduler')
        try:
            self.scheduler.remove_job(_id)
            _LOG.info('The job was successfully deregistered')
        except JobLookupError:
            _LOG.warning(f'Job with id \'{_id}\' was not found. Skipping')

    def update_job(self, item: ScheduledJob, is_enabled: Optional[bool] = None,
                   schedule: Optional[str] = None):
        _id = item.id
        _LOG.info(f'Updating scheduled job with id \'{_id}\'')
        job = self.scheduler.get_job(_id)
        # first update item in DB and second scheduler item. Not vice versa
        actions = []
        if isinstance(is_enabled, bool):
            actions.append(ScheduledJob.context['is_enabled'].set(is_enabled))
        if schedule:
            actions.append(ScheduledJob.context['schedule'].set(schedule))
        item.update(actions=actions)

        if isinstance(is_enabled, bool):
            job.resume() if is_enabled else job.pause()
        if schedule:
            job.reschedule(trigger=self.derive_trigger(schedule))
        _LOG.info(
            f'Scheduled job with name \'{_id}\' was successfully updated')

    def register_retry_job(self, event: dict, function):
        _LOG.error('Registering retry job. Not implemented. Nothing happens')

    # def register_retry_job(self, event: dict, function):
    #     # todo rewrite this mess
    #     job_id = event.get('schedule_job_id')
    #     retry_scheduler = self.scheduler.get_job(
    #         job_id=job_id) if job_id else None
    #     quota = 15
    #
    #     if self.setting_service.max_cron_number() <= len(
    #             self.scheduler.get_jobs()):
    #         _LOG.warning(f'Max cron number achieved: '
    #                      f'{self.setting_service.max_cron_number()}')
    #     elif not retry_scheduler:
    #         attempt = int(event.get('attempt', 1)) + 1
    #         path = event.pop('path')
    #         http_method = event.pop('httpMethod')
    #         for param in ('user_id', 'user_role', 'user_customer'):
    #             event.pop(param, None)
    #         new_event = {'attempt': attempt, 'headers': {'Host': ''},
    #                      'httpMethod': http_method,
    #                      'requestContext': {
    #                          'resourcePath': path, 'path': path},
    #                      'body': json.dumps(event)}
    #         hours = quota // 60 if attempt * quota >= 60 else None
    #         params = {'kwargs': {'event': new_event,
    #                              'context': RequestContext()},
    #                   'trigger': 'interval', 'minutes': quota % 60}
    #         if hours:
    #             params.update(hours=hours)
    #         retry_scheduler = self.scheduler.add_job(function, **params)
    #         new_event.update({'schedule_job_id': retry_scheduler.id})
    #         retry_scheduler.modify(
    #             name=RETRY_CRON_NAME_PATTERN + retry_scheduler.id,
    #             kwargs={'event': new_event, 'context': RequestContext()})
    #
    #         self.report_statistics_service.create(
    #             event, attempt=attempt, _id=retry_scheduler.id).save()
    #         _LOG.info(
    #             f'Scheduled job with id \'{retry_scheduler.id}\' '
    #             f'was successfully created. Time interval: {quota} minutes')
    #     elif retry_scheduler.kwargs['event'].get('attempt') == \
    #             self.setting_service.get_max_attempt_number():
    #         retries = self.report_statistics_service.get_by_id_and_no_status(
    #             retry_scheduler.id)
    #         with ReportStatistics.batch_write() as writer:
    #             for r in retries:
    #                 r.status = ReportDispatchStatus.PENDING
    #                 writer.save(r)
    #         for cron in self.scheduler.get_jobs():
    #             if cron.name.startswith(RETRY_CRON_NAME_PATTERN):
    #                 try:
    #                     self.scheduler.remove_job(cron.id)
    #                 except JobLookupError:
    #                     _LOG.warning(
    #                         f'Job {retry_scheduler.id} is already deleted')
    #         _LOG.debug('Disabling ability to send reports')
    #         self.setting_service.disable_send_reports()
    #     else:
    #         attempt = int(retry_scheduler.kwargs['event'].get(
    #             'attempt', 1)) + 1
    #
    #         retries = list(ReportStatistics.query(
    #             hash_key=retry_scheduler.id,
    #             range_key_condition=ReportStatistics.status.does_not_exist(),
    #             filter_condition=ReportStatistics.tenant == event.get(
    #                 'tenant_names') if event.get(
    #                 'tenant_names') else event.get(
    #                 'tenant_display_names'))
    #         )
    #         self.report_statistics_service.batch_update_status(
    #             'FAILED', retries)
    #         self.report_statistics_service.create(
    #             event, attempt=attempt, _id=retry_scheduler.id).save()
    #
    #         path = event.pop('path')
    #         event.pop('attempt', None)
    #         new_event = {'attempt': attempt, 'headers': {'Host': ''},
    #                      'httpMethod': event.pop('httpMethod', None),
    #                      'requestContext': {
    #                          'resourcePath': path, 'path': path},
    #                      'body': json.dumps(event)}
    #         hours = attempt * quota // 60 if attempt * quota >= 60 else None
    #         params = {'trigger': 'interval', 'minutes': attempt * quota % 60}
    #         if hours:
    #             params.update(hours=hours)
    #
    #         retry_scheduler = retry_scheduler.reschedule(**params)
    #         retry_scheduler.modify(kwargs={'event': new_event,
    #                                        'context': RequestContext()})
    #         _LOG.info(
    #             f'Scheduled job with id \'{retry_scheduler.id}\' '
    #             f'was successfully updated. New time interval: '
    #             f'{attempt * quota} minutes')
