from typing import Optional

from helpers.constants import STATUS_STARTING, STATUS_RUNNING, STATUS_FAILED, \
    STATUS_SUCCEEDED
from helpers.log_helper import get_logger
from helpers.time_helper import utc_iso
from models.job import Job
from models.scheduled_job import ScheduledJob
from services.environment_service import EnvironmentService
from services.license_manager_service import LicenseManagerService
from services.modular_service import TenantService

CREATED_AT_ATTR = 'created_at'
STARTED_AT_ATTR = 'started_at'
STOPPED_AT_ATTR = 'stopped_at'

STATUS_ATTR = 'status'

SCAN_RULESETS_ATTR = 'scan_rulesets'
SCAN_REGIONS_ATTR = 'scan_regions'

ALLOWED_TIMESTAMP_ATTRIBUTES = (
    CREATED_AT_ATTR, STARTED_AT_ATTR, STOPPED_AT_ATTR)

ALLOWED_STATUSES = (
    STATUS_STARTING, STATUS_RUNNING, STATUS_FAILED, STATUS_SUCCEEDED
)

_LOG = get_logger(__name__)


class JobUpdaterService:
    def __init__(self, environment_service: EnvironmentService,
                 license_manager_service: LicenseManagerService,
                 tenant_service: TenantService):
        self._environment_service = environment_service
        self._license_manager_service = license_manager_service
        self._tenant_service = tenant_service
        self._job = None

    @property
    def is_docker(self) -> bool:
        return self._environment_service.is_docker()

    def _create_job(self, **kwargs) -> Job:
        _tenant = self._tenant_service.get_tenant()
        params = dict(
            job_id=self._environment_service.batch_job_id(),
            job_owner=_tenant.customer_name,
            # because we cannot access user_id
            tenant_display_name=_tenant.name,
            customer_display_name=_tenant.customer_name,
            submitted_at=self._environment_service.submitted_at(),
            scheduled_rule_name=self._environment_service.scheduled_job_name())
        params.update(kwargs)
        return Job(**params)

    @property
    def job(self) -> Job:
        if not self._job:
            job_id = self._environment_service.batch_job_id()
            _LOG.info(f'Querying a job with id \'{job_id}\'')
            self._job = Job.get_nullable(hash_key=job_id)
            if not self._job:
                _LOG.warning(f'Job with id \'{job_id}\' does not exist in '
                             f'DB. Creating one')
                self._job = self._create_job()
        return self._job

    def _save(self):
        if isinstance(self._job, Job):
            self._job.save()

    def set_created_at(self):
        if self.is_docker:
            self.job.update(actions=[
                Job.created_at.set(utc_iso()),
                Job.status.set(STATUS_STARTING)
            ])

    def set_started_at(self):
        if self.is_docker:
            self.job.update(actions=[
                Job.started_at.set(utc_iso()),
                Job.status.set(STATUS_RUNNING),
                Job.scan_rulesets.set(
                    (self._environment_service.target_rulesets_view() or [] +
                     self._environment_service.licensed_ruleset_list())
                ),
                Job.scan_regions.set(
                    self._environment_service.target_regions())
            ])

    def set_failed_at(self, reason):
        self._set_stopped_at(STATUS_FAILED, reason)

    def set_succeeded_at(self):
        if self.is_docker:
            self._set_stopped_at(STATUS_SUCCEEDED)

    def _set_stopped_at(self, status: str, reason: str = None):
        assert status in {STATUS_SUCCEEDED, STATUS_FAILED}
        actions = [Job.stopped_at.set(utc_iso()), Job.status.set(status)]
        if reason:
            actions.append(Job.reason.set(reason))
        self.job.update(actions=actions)
        _job = self.job
        self.update_job_in_lm(
            job_id=_job.job_id,
            created_at=_job.created_at,
            started_at=_job.started_at,
            stopped_at=_job.stopped_at,
            status=_job.status
        )

    def update_job_in_lm(self, job_id: str, created_at: Optional[str] = None,
                         started_at: Optional[str] = None,
                         stopped_at: Optional[str] = None,
                         status: Optional[str] = None):
        # for saas the job in LM will be updated in caas-job-updater
        if (self._environment_service.is_licensed_job() and
                self._environment_service.is_docker()):
            _LOG.info(
                'The job is licensed on premises. Updating the job in LM')
            self._license_manager_service.update_job_in_license_manager(
                job_id=job_id,
                created_at=created_at,
                started_at=started_at,
                stopped_at=stopped_at,
                status=status
            )

    def update_scheduled_job(self):
        """
        Updates 'last_execution_time' in scheduled job item if
        this is a scheduled job.
        """
        _LOG.info('Updating scheduled job item in DB')
        scheduled_job_name = self._environment_service.scheduled_job_name()
        if scheduled_job_name:
            _LOG.info('The job is scheduled. Updating the '
                      '\'last_execution_time\' in scheduled job item')
            item = ScheduledJob(id=scheduled_job_name,
                                type=ScheduledJob.default_type)
            item.update(actions=[
                ScheduledJob.last_execution_time.set(utc_iso())
            ])
        else:
            _LOG.info('The job is not scheduled. No scheduled job '
                      'item to update. Skipping')
