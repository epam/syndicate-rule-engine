"""License Manager integration for job execution."""

from executor.job.job_failure import JobFailure, JobErrorCode
from executor.job.types import JobExecutionError
from helpers.log_helper import get_logger
from models.job import Job
from services import SP
from services.clients.lm_client import LMException
from services.ruleset_service import RulesetName

_LOG = get_logger(__name__)


def post_lm_job(job: Job) -> bool:
    if not job.affected_license:
        return False
    rulesets = list(
        filter(lambda x: x.license_key, [RulesetName(r) for r in job.rulesets])
    )
    if not rulesets:
        return False
    lk = rulesets[0].license_key
    lic = SP.license_service.get_nullable(lk)
    if not lic:
        return False

    try:
        SP.license_manager_service.cl.post_job(
            job_id=job.id,
            customer=job.customer_name,
            tenant=job.tenant_name,
            ruleset_map={
                lic.tenant_license_key(job.customer_name): [
                    r.to_str() for r in rulesets
                ]
            },
        )
    except LMException as e:
        raise JobExecutionError(
            JobFailure.standard(
                JobErrorCode.LM_DID_NOT_ALLOW,
                detail=str(e),
            )
        )

    return True
