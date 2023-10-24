import os
from typing import Optional

from helpers.constants import ENV_DEFAULT_BUCKET_NAME, ENV_JOB_ID, \
    ENV_TARGET_RULESETS, ENV_AWS_DEFAULT_REGION, ENV_VAR_CREDENTIALS, \
    ENV_VAR_REGION, ENV_VAR_JOB_LIFETIME_MIN, ENV_TARGET_RULESETS_VIEW, \
    ENV_AFFECTED_LICENSES, ENV_LICENSED_RULESETS, ENV_TARGET_REGIONS, \
    ENV_SUBMITTED_AT, ENV_SERVICE_MODE, DOCKER_SERVICE_MODE, \
    AWS_DEFAULT_REGION, ENV_EXECUTOR_MODE, CONCURRENT_EXECUTOR_MODE, \
    CONSISTENT_EXECUTOR_MODE, DEFAULT_JOB_LIFETIME_MIN, \
    ENVS_TO_HIDE, HIDDEN_ENV_PLACEHOLDER, ENV_SCHEDULED_JOB_NAME, \
    ENV_JOB_TYPE, STANDARD_JOB_TYPE, SCHEDULED_JOB_TYPE_FOR_ENV, \
    ENV_BATCH_RESULTS_ID, ENV_BATCH_RESULTS_IDS, \
    MULTI_ACCOUNT_EVENT_DRIVEN_JOB_TYPE, ENV_SYSTEM_CUSTOMER_NAME, \
    ENV_TENANT_NAME, ENV_VAR_STATS_S3_BUCKET_NAME, ENV_ALLOW_MANAGEMENT_CREDS, \
    ENV_VAR_RULESETS_BUCKET_NAME, ENV_PLATFORM_ID


class EnvironmentService:
    def __init__(self):
        self._environment = os.environ

    def override_environment(self, environs: dict) -> None:
        self._environment.update(environs)

    def reports_bucket_name(self):
        return self._environment.get(ENV_DEFAULT_BUCKET_NAME)

    def statistics_bucket_name(self):
        return self._environment.get(ENV_VAR_STATS_S3_BUCKET_NAME)

    def rulesets_bucket_name(self):
        return self._environment.get(ENV_VAR_RULESETS_BUCKET_NAME)

    def batch_job_id(self):
        return self._environment.get(ENV_JOB_ID)

    def batch_results_id(self) -> str:
        """
        Returns an id of BatchResults DB item. The job will get rules to
        scan from there and will put its results in there. Currently,
        it's intended to be used with event-driven jobs only
        """
        return self._environment.get(ENV_BATCH_RESULTS_ID)

    def batch_results_ids(self) -> set:
        env = self._environment.get(ENV_BATCH_RESULTS_IDS)
        if not env:
            return set()
        return set(env.split(','))

    def target_regions(self) -> list:
        regions = self._environment.get(ENV_TARGET_REGIONS)
        if regions:
            return [region.strip() for region in regions.split(',')]
        return []

    def target_rulesets(self) -> list:
        env = self._environment.get(ENV_TARGET_RULESETS)
        if isinstance(env, str):
            return env.split(',')
        return []

    def target_rulesets_view(self) -> list:
        """
        Returns target rulesets in human-readable view in case the
        necessary env exists. If it doesn't - returns the same as
        self.target_rulesets
        """
        rulesets_view = self._environment.get(ENV_TARGET_RULESETS_VIEW)
        if not isinstance(rulesets_view, str):
            return self.target_rulesets()
        return rulesets_view.split(',')

    def licensed_ruleset_map(self, license_key_list: list):
        reference_map = {}
        rulesets = self._environment.get(ENV_LICENSED_RULESETS)
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

    def licensed_ruleset_list(self):
        rulesets = self._environment.get(ENV_LICENSED_RULESETS) or ''
        output = []
        for each in rulesets.split(','):
            _, *ruleset_id = each.split(':', 1)
            if ruleset_id:
                output.append(ruleset_id[0])
        return output

    def affected_licenses(self):
        license_keys = self._environment.get(ENV_AFFECTED_LICENSES)
        if license_keys:
            return [each.strip() for each in license_keys.split(',')]

    def is_licensed_job(self) -> bool:
        """
        Returns true in case the job is licensed. A licensed job is the
        one which involves at least one licensed ruleset.
        """
        return bool(self.affected_licenses())

    def aws_default_region(self):
        return self._environment.get(ENV_AWS_DEFAULT_REGION,
                                     AWS_DEFAULT_REGION)

    def aws_region(self):
        return self._environment.get(ENV_VAR_REGION) or \
            self.aws_default_region()

    def credentials_key(self):
        return self._environment.get(ENV_VAR_CREDENTIALS)

    def job_lifetime_min(self):
        return int(self._environment.get(
            ENV_VAR_JOB_LIFETIME_MIN, DEFAULT_JOB_LIFETIME_MIN))

    def is_docker(self):
        return self._environment.get(ENV_SERVICE_MODE) == DOCKER_SERVICE_MODE

    def job_type(self) -> str:
        """
        Default job type is `standard`
        """
        return self._environment.get(ENV_JOB_TYPE) or STANDARD_JOB_TYPE

    def is_standard(self) -> bool:
        return self.job_type() == STANDARD_JOB_TYPE

    def is_multi_account_event_driven(self) -> bool:
        return self.job_type() == MULTI_ACCOUNT_EVENT_DRIVEN_JOB_TYPE

    def is_scheduled(self) -> bool:
        return self.job_type() == SCHEDULED_JOB_TYPE_FOR_ENV

    def submitted_at(self):
        return self._environment.get(ENV_SUBMITTED_AT)

    def is_concurrent(self) -> bool:
        mode = self._environment.get(ENV_EXECUTOR_MODE) or \
               CONSISTENT_EXECUTOR_MODE
        return mode == CONCURRENT_EXECUTOR_MODE

    def scheduled_job_name(self) -> Optional[str]:
        return self._environment.get(ENV_SCHEDULED_JOB_NAME) or None

    def system_customer(self) -> Optional[str]:
        """
        Currently used only for event-driven scans in order to retrieve
        ED system rulesets.
        """
        return self._environment.get(ENV_SYSTEM_CUSTOMER_NAME)

    def tenant_name(self) -> Optional[str]:
        """
        Standard (not event-driven) scans involve one tenant per job.
        This env contains this tenant's name
        """
        return self._environment.get(ENV_TENANT_NAME)

    def platform_id(self) -> Optional[str]:
        """
        We can scan platforms within tenants. In case platform id is
        provided, this specific platform must be scanned
        :return:
        """
        return self._environment.get(ENV_PLATFORM_ID)

    def is_management_creds_allowed(self) -> bool:
        """
        Specifies whether it's allowed to use Maestro's management
        credentials to scan a tenant. Default if False because it's not safe.
        Those creds have not only read access
        """
        return str(
            self._environment.get(ENV_ALLOW_MANAGEMENT_CREDS)
        ).lower() == 'true'

    def __repr__(self):
        return ', '.join([
            f'{k}={v if k not in ENVS_TO_HIDE else HIDDEN_ENV_PLACEHOLDER}'
            for k, v in self._environment.items()
        ])
