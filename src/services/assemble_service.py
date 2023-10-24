from typing import List, Optional, Tuple

from modular_sdk.models.tenant import Tenant

from helpers import get_logger
from helpers.constants import (
    BATCH_STANDARD_JOB_TYPE, BATCH_ENV_TARGET_REGIONS,
    BATCH_ENV_DEFAULT_REPORTS_BUCKET_NAME, BATCH_ENV_AWS_REGION,
    BATCH_ENV_CREDENTIALS_KEY, BATCH_ENV_JOB_LIFETIME_MIN,
    BATCH_ENV_JOB_TYPE, BATCH_ENV_LOG_LEVEL, BATCH_ENV_SUBMITTED_AT,
    BATCH_ENV_AFFECTED_LICENSES, BATCH_ENV_LICENSED_RULESETS,
    BATCH_ENV_TARGET_RULESETS_VIEW, BATCH_ENV_TARGET_RULESETS,
    BATCH_ENV_TENANT_NAME, BATCH_ENV_STATS_S3_BUCKET_NAME,
    BATCH_ENV_VAR_RULESETS_BUCKET_NAME, BATCH_ENV_PLATFORM_ID)
from helpers.time_helper import utc_iso
from services.environment_service import EnvironmentService

_LOG = get_logger(__name__)


class AssembleService:
    def __init__(self, environment_service: EnvironmentService):
        self.environment_service = environment_service

    def build_job_envs(self, tenant: Tenant,
                       platform_id: Optional[str] = None,
                       target_regions: Optional[List[str]] = None,
                       target_rulesets: Optional[
                           List[Tuple[str, str, str]]] = None,
                       affected_licenses: Optional[List[str]] = None,
                       licensed_rulesets: Optional[List[str]] = None,
                       job_type: Optional[str] = BATCH_STANDARD_JOB_TYPE,
                       credentials_key: Optional[str] = None
                       ) -> dict:
        # TODO +- duplicate in event_assembler_handler._build_common_envs
        envs = {
            BATCH_ENV_DEFAULT_REPORTS_BUCKET_NAME:
                self.environment_service.default_reports_bucket_name(),
            BATCH_ENV_STATS_S3_BUCKET_NAME:
                self.environment_service.get_statistics_bucket_name(),
            BATCH_ENV_VAR_RULESETS_BUCKET_NAME: self.environment_service.get_rulesets_bucket_name(),
            BATCH_ENV_AWS_REGION: self.environment_service.aws_region(),
            BATCH_ENV_JOB_LIFETIME_MIN:
                self.environment_service.get_job_lifetime_min(),
            BATCH_ENV_LOG_LEVEL:
                self.environment_service.batch_job_log_level(),
            BATCH_ENV_SUBMITTED_AT: utc_iso(),
            BATCH_ENV_TENANT_NAME: tenant.name,
            BATCH_ENV_PLATFORM_ID: platform_id,
            BATCH_ENV_CREDENTIALS_KEY: credentials_key,
            BATCH_ENV_JOB_TYPE: job_type,
            BATCH_ENV_TARGET_REGIONS: ','.join(target_regions or []),
        }
        if target_rulesets:
            envs[BATCH_ENV_TARGET_RULESETS] = ','.join(
                t[0] for t in target_rulesets)
            envs[BATCH_ENV_TARGET_RULESETS_VIEW] = ','.join(
                f'{t[1]}:{t[2]}' for t in target_rulesets
            )
        if affected_licenses and licensed_rulesets:
            envs.update({
                BATCH_ENV_AFFECTED_LICENSES: ','.join(affected_licenses),
                BATCH_ENV_LICENSED_RULESETS: ','.join(licensed_rulesets)
            })
        return self.sifted(envs)

    @staticmethod
    def sifted(dct: dict) -> dict:
        return {
            k: v for k, v in dct.items() if isinstance(v, (bool, int)) or v
        }
