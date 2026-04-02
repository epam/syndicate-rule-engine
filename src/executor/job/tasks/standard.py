"""Standard job Celery tasks."""

from typing import TYPE_CHECKING

from helpers.constants import BatchJobEnv, JobState
from helpers.log_helper import get_logger
from services import SP
from services.platform_service import Platform
from services.ruleset_service import RulesetName

from executor.job.execution.context import JobExecutionContext
from executor.job.execution.orchestrator import run_standard_job

if TYPE_CHECKING:
    from celery import Task

_LOG = get_logger(__name__)


def task_standard_job(self: 'Task | None', job_id: str):
    """
    Runs a single job by the given id
    """
    job = SP.job_service.get_nullable(job_id)
    if not job:
        _LOG.error('Task started for not existing job')
        return

    tenant = SP.modular_client.tenant_service().get(job.tenant_name)
    if not tenant:
        _LOG.error('Task started for not existing tenant')
        return
    platform = None
    if job.platform_id:
        parent = SP.modular_client.parent_service().get_parent_by_id(
            job.platform_id
        )
        if not parent:
            _LOG.error('Task started for not existing parent')
            return
        platform = Platform(parent)
    ctx = JobExecutionContext(job=job, tenant=tenant, platform=platform)
    with ctx:
        run_standard_job(ctx)


def task_scheduled_job(self: 'Task | None', customer_name: str, name: str) -> None:
    sch_job = SP.scheduled_job_service.get_by_name(
        customer_name=customer_name, name=name
    )
    if not sch_job:
        _LOG.error('Cannot start scheduled job for not existing sch item')
        return None
    tenant = SP.modular_client.tenant_service().get(sch_job.tenant_name)
    if not tenant:
        _LOG.error('Task started for not existing tenant')
        return None

    _LOG.info('Building job item from scheduled')
    rulesets = sch_job.meta.as_dict().get('rulesets', [])
    licensed = [r for r in map(RulesetName, rulesets) if r.license_key]
    license_key = licensed[0].license_key if licensed else None

    job = SP.job_service.create(
        customer_name=sch_job.customer_name,
        tenant_name=sch_job.tenant_name,
        regions=sch_job.meta.as_dict().get('regions', []),
        rulesets=sch_job.meta.as_dict().get('rulesets', []),
        rules_to_scan=[],
        affected_license=license_key,
        status=JobState.STARTING,
        batch_job_id=BatchJobEnv.JOB_ID.get(),
        celery_job_id=self.request.id if self is not None else None,
        scheduled_rule_name=name,
    )
    SP.job_service.save(job)

    ctx = JobExecutionContext(job=job, tenant=tenant, platform=None)
    with ctx:
        run_standard_job(ctx)
