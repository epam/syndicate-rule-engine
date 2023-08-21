from datetime import datetime, timedelta

import boto3

from helpers.log_helper import get_logger
from helpers.time_helper import utc_datetime
from services.environment_service import EnvironmentService

_LOG = get_logger(__name__)


class BatchService:
    def __init__(self, environment_service: EnvironmentService):
        self._environment = environment_service
        self._client = None

    @property
    def is_docker(self) -> bool:
        return self._environment.is_docker()

    @property
    def client(self):
        if not self._client:
            self._client = boto3.client(
                'batch', self._environment.aws_region())
        return self._client

    def get_time_left(self, job_lifetime_min=None):
        job_lifetime_min = job_lifetime_min or \
                           self._environment.job_lifetime_min()
        _LOG.debug('Retrieving \'startedAt\' timestamp')
        job_id = self._environment.batch_job_id()
        if self.is_docker:
            created_at = utc_datetime().timestamp() * 1e3
        else:
            job = self._get_job_by_id(job_id=self._environment.batch_job_id())
            created_at = None
            if job:
                created_at = job.get('startedAt')
            if not created_at:
                _LOG.warning(f'Can\'t find job with id {job_id}. Take current '
                             f'time as value of \'startedAt\' parameter')
                created_at = utc_datetime().timestamp() * 1e3
        threshold = datetime.timestamp(datetime.fromtimestamp(
            created_at / 1e3) + timedelta(minutes=job_lifetime_min))
        _LOG.debug(f'Threshold: {threshold}, '
                   f'{datetime.fromtimestamp(threshold)}')
        return threshold

    def _get_job_by_id(self, job_id):
        response = self.client.describe_jobs(jobs=[job_id])
        jobs = response.get('jobs', [])
        if jobs:
            return jobs[0]
        _LOG.warning(f'Can\'t find job with id {job_id}.')
