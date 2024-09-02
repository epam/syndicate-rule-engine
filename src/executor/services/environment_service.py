import os
from typing import cast

from executor.helpers.constants import (ENVS_TO_HIDE,
                                        HIDDEN_ENV_PLACEHOLDER)
from helpers.constants import (BatchJobEnv, BatchJobType, ENV_TRUE)
from services.environment_service import EnvironmentService


class BatchEnvironmentService(EnvironmentService):
    """
    Extends base environment with batch specific-ones
    """

    def aws_region(self) -> str:
        return BatchJobEnv.AWS_REGION.get(self.aws_default_region())

    def job_id(self) -> str | None:
        return BatchJobEnv.CUSTODIAN_JOB_ID.get()

    def batch_job_id(self) -> str:
        return cast(str, BatchJobEnv.JOB_ID.get())

    def batch_results_ids(self) -> set[str]:
        env = BatchJobEnv.BATCH_RESULTS_IDS.get()
        if not env:
            return set()
        return set(env.split(','))

    def target_regions(self) -> set[str]:
        """
        Definitely important for AWS scans. Returns regions that must be
        scanned by not global policies. Global one anyway will scan all the
        resources
        :return:
        """
        regions = BatchJobEnv.TARGET_REGIONS.get()
        if regions:
            return set(map(str.strip, regions.split(',')))
        return set()

    def affected_licenses(self) -> list[str]:
        license_keys = BatchJobEnv.AFFECTED_LICENSES.get()
        if license_keys:
            return [each.strip() for each in license_keys.split(',')]
        return []

    def is_licensed_job(self) -> bool:
        """
        Returns true in case the job is licensed. A licensed job is the
        one which involves at least one licensed ruleset.
        """
        return not not self.affected_licenses()

    def aws_default_region(self):
        return BatchJobEnv.AWS_DEFAULT_REGION.get()

    def credentials_key(self) -> str | None:
        return BatchJobEnv.CREDENTIALS_KEY.get()

    def job_lifetime_min(self) -> int:
        """
        Must return job lifetime in minutes
        :return:
        """
        return int(BatchJobEnv.BATCH_JOB_LIFETIME_MINUTES.get())

    def job_type(self) -> BatchJobType:
        """
        Default job type is `standard`
        """
        env = BatchJobEnv.JOB_TYPE.get(BatchJobType.STANDARD.value)
        return BatchJobType(env)

    def is_standard(self) -> bool:
        return self.job_type() == BatchJobType.STANDARD

    def is_multi_account_event_driven(self) -> bool:
        return self.job_type() == BatchJobType.EVENT_DRIVEN

    def is_scheduled(self) -> bool:
        return self.job_type() == BatchJobType.SCHEDULED

    def submitted_at(self):
        return BatchJobEnv.SUBMITTED_AT.get()

    def scheduled_job_name(self) -> str | None:
        return BatchJobEnv.SCHEDULED_JOB_NAME.get()

    def tenant_name(self) -> str | None:
        """
        Standard (not event-driven) scans involve one tenant per job.
        This env contains this tenant's name
        """
        return BatchJobEnv.TENANT_NAME.get()

    def platform_id(self) -> str | None:
        """
        We can scan platforms within tenants. In case platform id is
        provided, this specific platform must be scanned
        :return:
        """
        return BatchJobEnv.PLATFORM_ID.get()

    def is_management_creds_allowed(self) -> bool:
        """
        Specifies whether it's allowed to use Maestro's management
        credentials to scan a tenant. Default if False because it's not safe.
        Those creds have not only read access
        """
        return BatchJobEnv.ALLOW_MANAGEMENT_CREDS.get('').lower() in ENV_TRUE

    def __repr__(self):
        return ', '.join([
            f'{k}={v if k not in ENVS_TO_HIDE else HIDDEN_ENV_PLACEHOLDER}'
            for k, v in os.environ.items()
        ])
