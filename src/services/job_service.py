import uuid
from datetime import date, datetime, timedelta
from typing import Any, Callable

from pynamodb.expressions.condition import Condition
from pynamodb.pagination import ResultIterator

from helpers.constants import JobState, JobType
from helpers.time_helper import utc_iso
from models.job import Job
from services.base_data_service import BaseDataService
from services.platform_service import Platform
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
        job_type: JobType = JobType.STANDARD,
        status: JobState = JobState.SUBMITTED,
        batch_job_id: str | None = None,
        celery_job_id: str | None = None,
        scheduled_rule_name: str | None = None,
        credentials_key: str | None = None,
        application_id: str | None = None,
        dojo_structure: dict[str, str] | None = None,
    ) -> Job:
        if not dojo_structure:
            dojo_structure = {}
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
            job_type=job_type.value,
            status=status.value,
            batch_job_id=batch_job_id,
            celery_job_id=celery_job_id,
            scheduled_rule_name=scheduled_rule_name,
            created_at=utc_iso(),
            credentials_key=credentials_key,
            application_id=application_id,
            dojo_structure=dojo_structure,
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
        warnings: list[str] | None = None,
        dojo_structure: dict[str, str] | None = None,
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
        if warnings:
            actions.append(Job.warnings.set(warnings))
        if dojo_structure:
            actions.append(Job.dojo_structure.set(dojo_structure))
        if actions:
            job.update(actions)

    def get_by_customer_name(
        self,
        customer_name: str,
        job_id: str | None = None,
        job_type: JobType | None = None,
        job_types: set[JobType] | None = None,
        status: JobState | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        filter_condition: Condition | None = None,
        ascending: bool = False,
        limit: int | None = None,
        last_evaluated_key: dict | None = None,
    ) -> ResultIterator[Job]:

        if start and end:
            rkc = Job.submitted_at.between(utc_iso(start), utc_iso(end))
        elif start:
            rkc = Job.submitted_at >= utc_iso(start)
        elif end:
            rkc = Job.submitted_at <= utc_iso(end)
        else:
            rkc = None
        if status:
            filter_condition &= Job.status == status.value
        if job_id:
            filter_condition &= Job.id == job_id
        if job_type:
            filter_condition &= Job.job_type == job_type.value
        if job_types:
            filter_condition &= Job.job_type.is_in(*job_types)
        return Job.customer_name_submitted_at_index.query(
            hash_key=customer_name,
            range_key_condition=rkc,
            filter_condition=filter_condition,
            scan_index_forward=ascending,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
        )

    def get_by_job_types(
        self,
        job_types: set[JobType],
        job_id: str | None = None,
        customer_name: str | None = None,
        status: JobState | None = None,
        filter_condition: Condition | None = None,
        limit: int | None = None,
        last_evaluated_key: dict | None = None,
    ) -> ResultIterator[Job]:
        if status:
            filter_condition &= Job.status == status.value
        if job_id:
            filter_condition &= Job.id == job_id
        if customer_name:
            filter_condition &= Job.customer_name == customer_name
        if job_types:
            filter_condition &= Job.job_type.is_in(*job_types)
        return Job.scan(
            filter_condition=filter_condition,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
        )

    def get_by_tenant_name(
        self,
        tenant_name: str,
        job_type: JobType | None = None,
        job_types: set[JobType] | None = None,
        status: JobState | None = None,
        start: datetime | date | None = None,
        end: datetime | date | None = None,
        filter_condition: Condition | None = None,
        ascending: bool = False,
        limit: int | None = None,
        last_evaluated_key: dict | None = None,
    ) -> ResultIterator[Job]:
        if start and end:
            rkc = Job.submitted_at.between(utc_iso(start), utc_iso(end))
        elif start:
            rkc = Job.submitted_at >= utc_iso(start)
        elif end:
            rkc = Job.submitted_at <= utc_iso(end)
        else:
            rkc = None
        if status:
            filter_condition &= Job.status == status.value
        if job_type:
            filter_condition &= Job.job_type == job_type.value
        if job_types:
            filter_condition &= Job.job_type.is_in(*job_types)
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
        raw.pop('credentials_key', None)
        rulesets = []
        for r in item.rulesets:
            rulesets.append(RulesetName(r).to_str(False))
        raw['rulesets'] = rulesets
        dojo_structure = raw.pop('dojo_structure').as_dict()
        if dojo_structure:
            raw['dojo_structure'] = dojo_structure
        return raw

    def get_tenant_last_job_date(self, tenant_name: str) -> str | None:
        job = next(Job.tenant_name_submitted_at_index.query(
            hash_key=tenant_name,
            filter_condition=(Job.status == JobState.SUCCEEDED.value) & Job.platform_id.does_not_exist(),
            scan_index_forward=False,
            limit=1,
            attributes_to_get=(Job.submitted_at, )
        ), None)
        if not job:
            return
        return job.submitted_at

    def get_platform_last_job_date(self, platform: Platform) -> str | None:
        job = next(Job.tenant_name_submitted_at_index.query(
            hash_key=platform.tenant_name,
            filter_condition=(Job.status == JobState.SUCCEEDED.value) & Job.platform_id.exists(),
            scan_index_forward=False,
            limit=1,
            attributes_to_get=(Job.submitted_at, )
        ), None)
        if not job:
            return
        return job.submitted_at


class JobAttributeSetterDescriptor:
    __slots__ = ('_cb', '_name')

    def __init__(self, callback: Callable | None = None):
        self._cb = callback

    def __set_name__(self, owner, name):
        self._name = name

    def __set__(self, instance: 'JobUpdater', value):
        if self._cb:
            value = self._cb(value)
        instance._actions.append(getattr(Job, self._name).set(value))


class JobUpdater:
    """
    Allows to update job attributes more easily
    """

    def __init__(self, job: Job):
        self._job = job

        self._actions = []

    @classmethod
    def from_job_id(cls, job_id) -> 'JobUpdater':
        return JobUpdater(Job(id=job_id))

    def update(self):
        if not self._actions:
            return
        self._job.update(actions=self._actions)
        self._actions.clear()

    @property
    def job(self) -> Job:
        return self._job

    @staticmethod
    def _dt_callback(dt: datetime | str):
        if isinstance(dt, datetime):
            dt = utc_iso(dt)
        return dt

    status = JobAttributeSetterDescriptor(
        lambda x: x.value if isinstance(x, JobState) else x
    )
    reason = JobAttributeSetterDescriptor()
    created_at = JobAttributeSetterDescriptor(_dt_callback)
    started_at = JobAttributeSetterDescriptor(_dt_callback)
    stopped_at = JobAttributeSetterDescriptor(_dt_callback)
    queue = JobAttributeSetterDescriptor()
    definition = JobAttributeSetterDescriptor()
    rulesets = JobAttributeSetterDescriptor(lambda x: sorted(x))
    celery_task_id = JobAttributeSetterDescriptor()
    batch_job_id = JobAttributeSetterDescriptor()
    warnings = JobAttributeSetterDescriptor(lambda x: sorted(x))

    def add_warnings(self, *warns):
        self._actions.append(
            Job.warnings.set(Job.warnings.append(list(warns)))
        )
