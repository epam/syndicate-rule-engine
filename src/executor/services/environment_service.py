from typing_extensions import override

from executor.helpers.constants import (ExecutorMode, AWS_DEFAULT_REGION,
                                        DEFAULT_JOB_LIFETIME_MIN, ENVS_TO_HIDE,
                                        HIDDEN_ENV_PLACEHOLDER)
from helpers.constants import (BatchJobEnv, BatchJobType, ENV_TRUE)
from services.environment_service import EnvironmentService


class BatchEnvironmentService(EnvironmentService):
    """
    Extends base environment with batch specific-ones
    """

    @override
    def aws_region(self) -> str:
        return super().aws_region() or self.aws_default_region()

    def job_id(self) -> str | None:
        return self._environment.get(BatchJobEnv.CUSTODIAN_JOB_ID)

    def batch_job_id(self) -> str:
        return self._environment[BatchJobEnv.JOB_ID]

    def batch_results_ids(self) -> set[str]:
        env = self._environment.get(BatchJobEnv.BATCH_RESULTS_IDS)
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
        regions = self._environment.get(BatchJobEnv.TARGET_REGIONS)
        if regions:
            return set(map(str.strip, regions.split(',')))
        return set()

    def target_rulesets(self) -> set[str]:
        env = self._environment.get(BatchJobEnv.TARGET_RULESETS)
        if env:
            return set(map(str.strip, env.split(',')))
        return set()

    def licensed_ruleset_map(self, license_key_list: list[str]
                             ) -> dict[str, list[str]]:
        reference_map = {}
        rulesets = self._environment.get(BatchJobEnv.LICENSED_RULESETS)
        if rulesets:
            for each in rulesets.split(','):
                index, *ruleset_id = each.split(':', 1)
                try:
                    index = int(index)
                except ValueError:
                    continue
                if ruleset_id and 0 <= index < len(license_key_list):
                    key = license_key_list[index]
                    referenced = reference_map.setdefault(key, [])
                    referenced.append(ruleset_id[0])
        return reference_map

    def affected_licenses(self) -> list[str]:
        license_keys = self._environment.get(BatchJobEnv.AFFECTED_LICENSES)
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
        return self._environment.get(
            BatchJobEnv.AWS_DEFAULT_REGION) or AWS_DEFAULT_REGION

    def credentials_key(self) -> str | None:
        return self._environment.get(BatchJobEnv.CREDENTIALS_KEY)

    def job_lifetime_min(self) -> int:
        """
        Must return job lifetime in minutes
        :return:
        """
        return int(self._environment.get(
            BatchJobEnv.BATCH_JOB_LIFETIME_MINUTES, DEFAULT_JOB_LIFETIME_MIN
        ))

    def job_type(self) -> BatchJobType:
        """
        Default job type is `standard`
        """
        env = self._environment.get(BatchJobEnv.JOB_TYPE)
        if env:
            return BatchJobType(env)
        return BatchJobType.STANDARD

    def is_standard(self) -> bool:
        return self.job_type() == BatchJobType.STANDARD

    def is_multi_account_event_driven(self) -> bool:
        return self.job_type() == BatchJobType.EVENT_DRIVEN

    def is_scheduled(self) -> bool:
        return self.job_type() == BatchJobType.SCHEDULED

    def submitted_at(self):
        return self._environment.get(BatchJobEnv.SUBMITTED_AT)

    def executor_mode(self) -> ExecutorMode:
        _default = ExecutorMode.CONSISTENT
        env = self._environment.get(BatchJobEnv.EXECUTOR_MODE)
        if not env:
            return _default
        try:
            return ExecutorMode(env)
        except ValueError:
            return _default

    def is_concurrent(self) -> bool:
        return self.executor_mode() == ExecutorMode.CONCURRENT

    def scheduled_job_name(self) -> str | None:
        return self._environment.get(BatchJobEnv.SCHEDULED_JOB_NAME) or None

    def tenant_name(self) -> str | None:
        """
        Standard (not event-driven) scans involve one tenant per job.
        This env contains this tenant's name
        """
        return self._environment.get(BatchJobEnv.TENANT_NAME)

    def platform_id(self) -> str | None:
        """
        We can scan platforms within tenants. In case platform id is
        provided, this specific platform must be scanned
        :return:
        """
        return self._environment.get(BatchJobEnv.PLATFORM_ID)

    def is_management_creds_allowed(self) -> bool:
        """
        Specifies whether it's allowed to use Maestro's management
        credentials to scan a tenant. Default if False because it's not safe.
        Those creds have not only read access
        """
        return str(
            self._environment.get(BatchJobEnv.ALLOW_MANAGEMENT_CREDS)
        ).lower() in ENV_TRUE

    def __repr__(self):
        return ', '.join([
            f'{k}={v if k not in ENVS_TO_HIDE else HIDDEN_ENV_PLACEHOLDER}'
            for k, v in self._environment.items()
        ])
