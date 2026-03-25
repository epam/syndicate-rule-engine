"""Job execution context manager. Handles job lifecycle, locks, and cleanup."""

import tempfile
from pathlib import Path
from typing import Optional, cast

from celery.exceptions import SoftTimeLimitExceeded
from modular_sdk.models.tenant import Tenant

from executor.helpers.constants import ExecutorError
from executor.job.types import ExecutorException
from helpers.constants import Cloud, Env, JobState
from helpers.log_helper import get_logger
from helpers.time_helper import utc_iso
from models.job import Job
from services import SP
from services.job_lock import TenantSettingJobLock
from services.job_service import JobUpdater
from services.platform_service import Platform

_LOG = get_logger(__name__)


class JobExecutionContext:
    def __init__(
        self,
        job: Job,
        tenant: Tenant,
        platform: Platform | None = None,
        cache: str | None = 'memory',
        cache_period: int = 30,
    ):
        self.job = job
        self.tenant = tenant
        self.platform = platform
        self.cache = cache
        self.cache_period = cache_period

        self.updater = JobUpdater(job)
        self._lm_job_posted: Optional[bool] = None

        self._work_dir = None
        self._exit_code = 0

        self.fingerprint_aliases: dict[str, list[str]] = {}

    def set_lm_job_posted(self, posted: bool, /) -> None:
        if not posted:
            _LOG.warning('License manager job was not posted')
        self._lm_job_posted = posted

    def is_platform_job(self) -> bool:
        return self.platform is not None

    def is_scheduled_job(self) -> bool:
        return self.job.scheduled_rule_name is not None

    def cloud(self) -> Cloud:
        if self.is_platform_job():
            return Cloud.KUBERNETES
        return cast(Cloud, Cloud.parse(self.tenant.cloud))

    @property
    def work_dir(self) -> Path:
        if not self._work_dir:
            raise RuntimeError('can be used only within context')
        return Path(self._work_dir.name)

    def add_warnings(self, *warnings: str) -> None:
        self.updater.add_warnings(*warnings)
        self.updater.update()

    def __enter__(self):
        _LOG.info(f'Acquiring lock for job {self.job.id}')
        TenantSettingJobLock(self.tenant.name).acquire(
            job_id=self.job.id, regions=self.job.regions
        )
        _LOG.info('Setting job status to RUNNING')
        self.updater.started_at = utc_iso()
        self.updater.status = JobState.RUNNING
        self.updater.update()

        _LOG.info('Creating a working dir')
        self._work_dir = tempfile.TemporaryDirectory()

    def _cleanup_cache(self) -> None:
        if self.cache is None or self.cache == 'memory':
            return
        f = Path(self.cache)
        if f.exists():
            f.unlink(missing_ok=True)

    def _cleanup_work_dir(self) -> None:
        if not self._work_dir:
            return
        self._work_dir.cleanup()
        self._work_dir = None

    def _update_lm_job(self):
        if not self.job.affected_license or not Env.is_docker():
            return
        _LOG.info('Updating job in license manager')
        SP.license_manager_service.client.update_job(
            job_id=self.job.id,
            customer=self.job.customer_name,
            created_at=self.job.created_at,
            started_at=self.job.started_at,
            stopped_at=self.job.stopped_at,
            status=self.job.status,
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        _LOG.info('Cleaning cache after job')
        self._cleanup_cache()
        _LOG.info('Cleaning work dir')
        self._cleanup_work_dir()
        _LOG.info('Releasing job lock')
        TenantSettingJobLock(self.tenant.name).release(self.job.id)

        if exc_val is None:
            _LOG.info(
                f'Job {self.job.id} finished without exceptions. Setting SUCCEEDED status'
            )
            self.updater.status = JobState.SUCCEEDED
            self.updater.stopped_at = utc_iso()
            self.updater.update()
            if self._lm_job_posted:
                self._update_lm_job()
            return

        _LOG.info(
            f'Job {self.job.id} finished with exception. Setting FAILED status'
        )
        self.updater.status = JobState.FAILED
        self.updater.stopped_at = utc_iso()
        if isinstance(exc_val, ExecutorException):
            _LOG.exception(f'Executor exception {exc_val} occurred')
            self.updater.reason = exc_val.error.get_reason()
            if exc_val.error.value == ExecutorError.LM_DID_NOT_ALLOW.value:
                self._exit_code = 2
            else:
                self._exit_code = 1
        elif isinstance(exc_val, SoftTimeLimitExceeded):
            _LOG.error('Job is terminated because of soft timeout')
            self.updater.reason = ExecutorError.TIMEOUT.get_reason()
            self._exit_code = 1
        else:
            _LOG.exception('Unexpected error occurred')
            self.updater.reason = ExecutorError.INTERNAL.get_reason()
            self._exit_code = 1

        _LOG.info('Updating job status')
        self.updater.update()
        if self._lm_job_posted:
            self._update_lm_job()
        return True
