from executor.job.execution.context import JobExecutionContext
from services import SP
from services.platform_service import Platform
from helpers.log_helper import get_logger

_LOG = get_logger(__name__)


def setup_job_execution_context(job_id: str) -> JobExecutionContext | None:
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

    return JobExecutionContext(job=job, tenant=tenant, platform=platform)
