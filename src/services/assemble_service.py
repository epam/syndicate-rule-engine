from typing import TYPE_CHECKING

from modular_sdk.models.tenant import Tenant

from helpers import sifted
from helpers.constants import CAASEnv, BatchJobEnv, BatchJobType
from helpers.log_helper import get_logger

if TYPE_CHECKING:
    from services.environment_service import EnvironmentService

_LOG = get_logger(__name__)


class AssembleService:
    def __init__(self, environment_service: 'EnvironmentService'):
        self.environment_service = environment_service

    def build_job_envs(self, tenant: Tenant, job_id: str = None,
                       platform_id: str = None,
                       target_regions: list[str] = None,
                       affected_licenses: list[str] | str | None = None,
                       job_type: BatchJobType = BatchJobType.STANDARD,
                       credentials_key: str = None,
                       job_lifetime_minutes: float | None = None) -> dict:
        # TODO +- duplicate in event_assembler_handler._build_common_envs
        envs = {
            CAASEnv.REPORTS_BUCKET_NAME:
                self.environment_service.default_reports_bucket_name(),
            CAASEnv.STATISTICS_BUCKET_NAME:
                self.environment_service.get_statistics_bucket_name(),
            CAASEnv.RULESETS_BUCKET_NAME:
                self.environment_service.get_rulesets_bucket_name(),
            CAASEnv.AWS_REGION: self.environment_service.aws_region(),
            CAASEnv.BATCH_JOB_LIFETIME_MINUTES: job_lifetime_minutes or self.environment_service.get_job_lifetime_min(),
            CAASEnv.LM_TOKEN_LIFETIME_MINUTES:
                str(self.environment_service.lm_token_lifetime_minutes()),
            'LOG_LEVEL': self.environment_service.batch_job_log_level(),
            BatchJobEnv.TENANT_NAME: tenant.name,
            BatchJobEnv.PLATFORM_ID: platform_id,
            BatchJobEnv.CREDENTIALS_KEY: credentials_key,
            BatchJobEnv.JOB_TYPE: job_type.value if isinstance(job_type, BatchJobType) else job_type,  # noqa
            BatchJobEnv.TARGET_REGIONS: ','.join(target_regions or []),
            BatchJobEnv.CUSTODIAN_JOB_ID: job_id,

        }
        if affected_licenses:
            if isinstance(affected_licenses, str):
                affected_licenses = [affected_licenses]
            envs.update({
                BatchJobEnv.AFFECTED_LICENSES: ','.join(affected_licenses),
            })
        return sifted(envs)
