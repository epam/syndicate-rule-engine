import os
from typing import Optional

from apscheduler.jobstores.base import ConflictingIdError, JobLookupError
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.base import BaseTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.util import datetime_to_utc_timestamp
from bson.binary import Binary
from modular_sdk.models.pynamodb_extension.base_model import build_mongodb_uri
from models.modular.tenants import Tenant
from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError

from connections.batch_extension.base_job_client import SUBPROCESS_HANDLER
from helpers import build_response, RESPONSE_BAD_REQUEST_CODE, \
    RESPONSE_RESOURCE_NOT_FOUND_CODE
from helpers.constants import BATCH_ENV_SCHEDULED_JOB_NAME
from helpers.constants import CUSTODIAN_TYPE, SCHEDULED_JOB_TYPE
from helpers.constants import ENV_MONGODB_DATABASE, ENV_MONGODB_USER, \
    ENV_MONGODB_PASSWORD, ENV_MONGODB_URL
from helpers.log_helper import get_logger
from helpers.time_helper import utc_iso
from models.scheduled_job import SJ_ID_ATTR, SJ_CREATION_DATE_ATTR, \
    SJ_TYPE_ATTR, SJ_CONTEXT_ATTR, SJ_LAST_EXECUTION_TIME_ATTR
from models.scheduled_job import ScheduledJob, SCHEDULED_JOBS_TABLE_NAME
from services.clients.scheduler import AbstractJobScheduler

try:
    import cPickle as pickle
except ImportError:  # pragma: nocover
    import pickle

_LOG = get_logger(__name__)

RATE_EXPRESSION_UNITS = {
    'minute', 'minutes', 'hour', 'hours', 'day', 'days', 'week', 'weeks',
    'second', 'seconds',
}


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
    def __init__(self):
        self._scheduler = None

    @property
    def scheduler(self) -> BackgroundScheduler:
        """
        Mock this in order not to init MONGODB_HANDLER in tests
        """
        if not self._scheduler:
            self._scheduler = BackgroundScheduler()
            self._scheduler.configure(jobstores={
                'default': CustomMongoDBJobStore(
                    database=os.getenv(ENV_MONGODB_DATABASE),
                    collection=SCHEDULED_JOBS_TABLE_NAME,
                    client=MongoClient(build_mongodb_uri(
                        os.getenv(ENV_MONGODB_USER),
                        os.getenv(ENV_MONGODB_PASSWORD),
                        os.getenv(ENV_MONGODB_URL)
                    )))  # or BaseModel.mongodb_handler().mongodb.client
            })
        return self._scheduler

    def start(self):
        self.scheduler.start()

    @staticmethod
    def _valid_cron(schedule: str) -> CronTrigger:
        """
        May raise ValueError
        """
        return CronTrigger.from_crontab(
            schedule.strip().replace('cron', '').strip('()'))

    @staticmethod
    def _valid_interval(schedule: str) -> IntervalTrigger:
        """
        May raise ValueError
        """
        value, unit = schedule.strip().replace('rate', '').strip('()').split()
        value = int(value)
        if unit not in RATE_EXPRESSION_UNITS:
            raise ValueError(
                f'Not available unit: \'{unit}\'. '
                f'Available: {", ".join(RATE_EXPRESSION_UNITS)}')
        if not unit.endswith('s'):
            unit = unit + 's'
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
            return build_response(
                code=RESPONSE_BAD_REQUEST_CODE,
                content=f'Schedule expression validation error: {error}')

    def register_job(self, tenant: Tenant, schedule: str,
                     environment: dict,
                     name: Optional[str] = None) -> ScheduledJob:
        _id = self.safe_name(name) if name else \
            self.safe_name_from_tenant(tenant)
        environment[BATCH_ENV_SCHEDULED_JOB_NAME] = _id
        _LOG.info(f'Registering new scheduled job with id \'{_id}\'')
        self.scheduler.add_job(
            SUBPROCESS_HANDLER().submit_job, id=_id, kwargs={
                'environment_variables': environment
            }, trigger=self.derive_trigger(schedule))
        # here it's not a mistake. self.scheduler.add_job above creates an
        # item in MongoDB, so we just query it
        _job = ScheduledJob.get_nullable(hash_key=_id)
        _LOG.info('Updating the created job item with some '
                  'Custodian`s required attributes')
        self._update_job_obj_with(_job, tenant, schedule, environment)
        _job.save()
        _LOG.debug('Scheduled job`s data was saved to Dynamodb')
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
        if not job:
            _LOG.warning(f'Job with id \'{_id}\' was not found')
            return build_response(
                code=RESPONSE_RESOURCE_NOT_FOUND_CODE,
                content=f'Cannot find rule for scheduled job \'{_id}\'. '
                        f'Recreate the job')
        # first update item in DB and second scheduler item. Not vice versa
        item.update_with(is_enabled=is_enabled, schedule=schedule)
        item.save()

        if isinstance(is_enabled, bool):
            job.resume() if is_enabled else job.pause()
        if schedule:
            job.reschedule(trigger=self.derive_trigger(schedule))
        _LOG.info(
            f'Scheduled job with name \'{_id}\' was successfully updated')
