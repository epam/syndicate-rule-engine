import uuid
from datetime import datetime, timedelta
from typing import Any

from pynamodb.expressions.condition import Condition
from pynamodb.pagination import ResultIterator

from helpers.constants import BatchJobEnv, JobState
from helpers.time_helper import utc_datetime, utc_iso
from models.job import Job
from services import SP
from services.base_data_service import BaseDataService
from services.ruleset_service import RulesetName


class JobService(BaseDataService[Job]):
    def create(
        self,
        customer_name: str,
        tenant_name: str,
        regions: list[str],
        rulesets: list[str],
        rules_to_scan: list[str] | None = None,
        platform_id: str | None = None,
        ttl: timedelta | None = None,
        owner: str | None = None,
        affected_license: str | None = None,
        status: JobState = JobState.SUBMITTED
    ) -> Job:
        return super().create(
            id=str(uuid.uuid4()),
            customer_name=customer_name,
            tenant_name=tenant_name,
            regions=regions,
            rulesets=rulesets,
            rules_to_scan=rules_to_scan or [],
            platform_id=platform_id,
            ttl=ttl,
            owner=owner,
            affected_license=affected_license,
            status=status.value
        )

    def update(
        self,
        job: Job,
        batch_job_id: str | None = None,
        celery_task_id: str | None = None,
        reason: str | None = None,
        status: JobState | None = None,
        created_at: str | None = None,
        started_at: str | None = None,
        stopped_at: str | None = None,
        queue: str | None = None,
        definition: str | None = None,
        rulesets: list[str] | None = None,
    ):
        actions = []
        if batch_job_id:
            actions.append(Job.batch_job_id.set(batch_job_id))
        if celery_task_id:
            actions.append(Job.celery_task_id.set(celery_task_id))
        if reason:
            actions.append(Job.reason.set(reason))
        if status:
            actions.append(Job.status.set(status.value))
        if created_at:
            actions.append(Job.created_at.set(created_at))
        if started_at:
            actions.append(Job.started_at.set(started_at))
        if stopped_at:
            actions.append(Job.stopped_at.set(stopped_at))
        if queue:
            actions.append(Job.queue.set(queue))
        if definition:
            actions.append(Job.definition.set(definition))
        if rulesets:
            actions.append(Job.rulesets.set(rulesets))
        if actions:
            job.update(actions)

    def get_by_customer_name(
        self,
        customer_name: str,
        status: JobState | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        filter_condition: Condition | None = None,
        ascending: bool = False,
        limit: int | None = None,
        last_evaluated_key: dict | None = None,
    ) -> ResultIterator[Job]:
        rkc = None
        if start and end:
            rkc = Job.submitted_at.between(
                lower=utc_iso(start), upper=utc_iso(end)
            )
        elif start:
            rkc = Job.submitted_at >= utc_iso(start)
        elif end:
            rkc = Job.submitted_at < utc_iso(end)
        else:
            rkc = None
        if status:
            filter_condition &= Job.status == status.value
        return Job.customer_name_submitted_at_index.query(
            hash_key=customer_name,
            range_key_condition=rkc,
            filter_condition=filter_condition,
            scan_index_forward=ascending,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
        )

    def get_by_tenant_name(
        self,
        tenant_name: str,
        status: JobState | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        filter_condition: Condition | None = None,
        ascending: bool = False,
        limit: int | None = None,
        last_evaluated_key: dict | None = None,
    ) -> ResultIterator[Job]:
        if start and end:
            rkc = Job.submitted_at.between(
                lower=utc_iso(start), upper=utc_iso(end)
            )
        elif start:
            rkc = Job.submitted_at >= utc_iso(start)
        elif end:
            rkc = Job.submitted_at < utc_iso(end)
        else:
            rkc = None
        if status:
            filter_condition &= Job.status == status.value
        return Job.tenant_name_submitted_at_index.query(
            hash_key=tenant_name,
            range_key_condition=rkc,
            filter_condition=filter_condition,
            scan_index_forward=ascending,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
        )

    def dto(self, item: Job) -> dict[str, Any]:
        raw = super().dto(item)
        raw.pop('batch_job_id', None)
        raw.pop('queue', None)
        raw.pop('definition', None)
        raw.pop('owner', None)
        raw.pop('rules_to_scan', None)
        raw.pop('ttl', None)
        raw.pop('celery_task_id', None)
        rulesets = []
        for r in item.rulesets:
            rulesets.append(RulesetName(r).to_str(False))
        raw['rulesets'] = rulesets
        return raw


class NullJobUpdater:
    """
    For standard jobs (not scheduled and not event-driven). Standard jobs
    are updated by caas-job-updater
    """

    def __init__(self, job: Job):
        self._job = job

    def save(self):
        pass

    def update(self):
        pass

    @property
    def job(self) -> Job:
        return self._job


class JobUpdater:
    """
    Allows to update job attributes more easily
    """

    __slots__ = ('_job', '_actions')

    def __init__(self, job: Job):
        self._job = job

        self._actions = []

    @classmethod
    def from_batch_env(
        cls, environment: dict[str, str], rulesets: list[str] | None = None
    ) -> 'JobUpdater':
        """
        A situation when the job does not exist in db is possible
        :param environment:
        :param rulesets:
        :return:
        """
        rulesets = rulesets or []
        licensed = [r for r in map(RulesetName, rulesets) if r.license_key]
        license_key = licensed[0].license_key if licensed else None

        tenant = SP.modular_client.tenant_service().get(
            environment[BatchJobEnv.TENANT_NAME.value]
        )
        submitted_at = utc_iso()
        if BatchJobEnv.SUBMITTED_AT.value in environment:
            submitted_at = utc_iso(
                utc_datetime(environment[BatchJobEnv.SUBMITTED_AT.value])
            )

        if not rulesets:
            rulesets = []
        return JobUpdater(
            Job(
                id=environment.get(BatchJobEnv.CUSTODIAN_JOB_ID.value)
                or str(uuid.uuid4()),
                batch_job_id=environment.get(BatchJobEnv.JOB_ID.value),
                tenant_name=tenant.name,
                customer_name=tenant.customer_name,
                submitted_at=submitted_at,
                status=JobState.SUBMITTED.value,
                owner=tenant.customer_name,
                regions=environment.get(
                    BatchJobEnv.TARGET_REGIONS.value, ''
                ).split(','),
                rulesets=rulesets,
                scheduled_rule_name=environment.get(
                    BatchJobEnv.SCHEDULED_JOB_NAME.value
                ),
                affected_license=license_key,
            )
        )

    @classmethod
    def from_job_id(cls, job_id) -> 'JobUpdater':
        return JobUpdater(Job(id=job_id))

    def save(self):
        self._job.save()

    def update(self):
        if not self._actions:
            return
        self._job.update(actions=self._actions)
        self._actions.clear()

    @property
    def job(self) -> Job:
        return self._job

    def status(self, status: str | JobState):
        if isinstance(status, str):
            status = JobState(status)
        self._actions.append(Job.status.set(status.value))

    def reason(self, reason: str | None):
        self._actions.append(Job.reason.set(reason))

    def created_at(self, created_at: datetime | str):
        if isinstance(created_at, datetime):
            created_at = utc_iso(created_at)
        self._actions.append(Job.created_at.set(created_at))

    def started_at(self, started_at: datetime | str):
        if isinstance(started_at, datetime):
            started_at = utc_iso(started_at)
        self._actions.append(Job.started_at.set(started_at))

    def stopped_at(self, stopped_at: datetime | str):
        if isinstance(stopped_at, datetime):
            stopped_at = utc_iso(stopped_at)
        self._actions.append(Job.stopped_at.set(stopped_at))

    def queue(self, queue: str):
        self._actions.append(Job.queue.set(queue))

    def definition(self, definition: str):
        self._actions.append(Job.definition.set(definition))

    status = property(None, status)
    reason = property(None, reason)
    created_at = property(None, created_at)
    started_at = property(None, started_at)
    stopped_at = property(None, stopped_at)
    queue = property(None, queue)
    definition = property(None, definition)
