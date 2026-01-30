import os
from typing import Mapping

from helpers.constants import (
    DOCKER_SERVICE_MODE,
    Env,
)


class EnvironmentService:
    def override_environment(self, environs: Mapping) -> None:
        os.environ.update(environs)

    def aws_region(self) -> str:
        """
        api-handler, event-handler to build envs for jobs.
        All the lambdas to init connections to clients.
        """
        return Env.AWS_REGION.get()

    def default_reports_bucket_name(self) -> str:
        """
        Lambdas:
        - event-handler
        - api-handler
        - report-generator
        """
        return Env.REPORTS_BUCKET_NAME.as_str()

    def batch_job_log_level(self) -> str:
        """
        Lambdas:
        api-handler
        event-handler
        """
        return Env.BATCH_JOB_LOG_LEVEL.get()

    def get_batch_job_queue(self) -> str:
        """
        Lambdas:
        api-handler
        event-handler
        """
        return Env.BATCH_JOB_QUEUE_NAME.as_str()

    def get_batch_job_def(self) -> str:
        """
        Lambdas:
        api-handler
        event-handler
        """
        return Env.BATCH_JOB_DEF_NAME.as_str()

    def get_rulesets_bucket_name(self) -> str:
        """
        Lambdas:
        api-handler
        configuration-api-handler
        configuration-updater
        event-handler
        license-updater
        report-generator
        ruleset-compiler
        """
        return Env.RULESETS_BUCKET_NAME.as_str()

    def get_user_pool_name(self) -> str | None:
        """
        Api lambdas:
        api-handler
        configuration-api-handler
        report-generator
        """
        return Env.USER_POOL_NAME.get()

    def get_user_pool_id(self) -> str | None:
        """
        It's optional but is preferred to use this instead of user_pool_name
        Api lambdas:
        api-handler
        configuration-api-handler
        report-generator
        """
        return Env.USER_POOL_ID.get()

    def get_statistics_bucket_name(self) -> str:
        return Env.STATISTICS_BUCKET_NAME.as_str()

    def skip_cloud_identifier_validation(self) -> bool:
        """
        api-handler
        """
        return Env.SKIP_CLOUD_IDENTIFIER_VALIDATION.as_bool()

    def is_docker(self) -> bool:
        return Env.is_docker()

    def event_bridge_service_role(self) -> str | None:
        return Env.EB_SERVICE_ROLE_TO_INVOKE_BATCH.get()

    def lambdas_alias_name(self) -> str | None:
        """
        To be able to trigger the valid lambda
        :return:
        """
        return Env.LAMBDA_ALIAS_NAME.get()

    def account_id(self) -> str | None:
        # resolved from lambda context
        return Env.ACCOUNT_ID.get()

    def jobs_time_to_live_days(self) -> int | None:
        """live_days
        Lambdas:
        - api-handler
        """
        from_env = Env.JOBS_TIME_TO_LIVE_DAYS.get('')
        if from_env.isdigit():
            return int(from_env)
        return

    def events_ttl_hours(self) -> int:
        """
        Lambdas:
        - api-handler
        """
        return Env.EVENTS_TTL_HOURS.as_int()

    def event_assembler_pull_item_limit(self) -> int:
        """
        Lambdas:
        - event-handler
        """
        return Env.EVENT_ASSEMBLER_PULL_EVENTS_PAGE_SIZE.as_int()

    def number_of_native_events_in_event_item(self) -> int:
        """
        Lambdas:
        - api-handler
        """
        return Env.NATIVE_EVENTS_PER_ITEM.as_int()

    def get_recommendation_bucket(self) -> str:
        return Env.RECOMMENDATIONS_BUCKET_NAME.as_str()

    def allow_simultaneous_jobs_for_one_tenant(self) -> bool:
        """
        api-handler. Here we are talking about standard licensed
        jobs, not event-driven.
        :return:
        """
        return Env.ALLOW_SIMULTANEOUS_JOBS_FOR_ONE_TENANT.as_bool()

    def number_of_partitions_for_events(self) -> int:
        """
        https://aws.amazon.com/blogs/database/choosing-the-right-dynamodb-partition-key/
        We must be able to query SREEvents starting from some date. We
        cannot just scan the table, and also we cannot use one Partition key
        for all the events. So this setting defines the number of partitions.
        The more of them, the better will be writing throughput and harder read
        :return:
        """
        return Env.NUMBER_OF_PARTITIONS_FOR_EVENTS.as_int()

    def lm_token_lifetime_minutes(self) -> int:
        return Env.LM_TOKEN_LIFETIME_MINUTES.as_int()

    def allow_disabled_permissions(self) -> bool:
        return Env.ALLOW_DISABLED_PERMISSIONS_FOR_STANDARD_USERS.as_bool()

    def minio_presigned_url_host(self) -> str | None:
        host = Env.MINIO_PRESIGNED_URL_HOST.get()
        if host:
            return host.strip().strip('/')
