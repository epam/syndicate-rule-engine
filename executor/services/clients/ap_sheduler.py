import os
from apscheduler.jobstores.base import JobLookupError
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from helpers.constants import ENV_MONGODB_DATABASE
from helpers.log_helper import get_logger
from models.modular import BaseModel
from models.scheduled_job import ScheduledJob, SCHEDULED_JOBS_TABLE_NAME
from services.clients.scheduler import AbstractJobScheduler

_LOG = get_logger(__name__)


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
    ...


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
                    client=BaseModel.mongodb_handler().mongodb.client
                )
            })
        return self._scheduler

    def update_job(self, item: ScheduledJob, is_enabled: bool):
        _id = item.id
        _LOG.info(f'Updating scheduled job with id \'{_id}\'')
        item.update_with(is_enabled=is_enabled)
        item.save()
        try:
            if is_enabled:
                self.scheduler.resume_job(_id)
            else:
                self.scheduler.pause_job(_id)
        except JobLookupError:
            _LOG.warning(f'Job with id \'{_id}\' was not found')
            item = None

        _LOG.info(
            f'Scheduled job with name \'{_id}\' was successfully updated'
        )

        return item
