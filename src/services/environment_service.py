import os
import re
from typing import Mapping

from helpers.constants import (
    CAASEnv,
    DEFAULT_EVENTS_TTL_HOURS,
    DEFAULT_INNER_CACHE_TTL_SECONDS,
    DEFAULT_LM_TOKEN_LIFETIME_MINUTES,
    DEFAULT_METRICS_BUCKET_NAME,
    DEFAULT_NUMBER_OF_EVENTS_IN_EVENT_ITEM,
    DEFAULT_NUMBER_OF_PARTITIONS_FOR_EVENTS,
    DEFAULT_RECOMMENDATION_BUCKET_NAME,
    DEFAULT_REPORTS_BUCKET_NAME,
    DEFAULT_RULESETS_BUCKET_NAME,
    DEFAULT_STATISTICS_BUCKET_NAME,
    DOCKER_SERVICE_MODE,
    ENV_TRUE,
)


class EnvironmentService:
    def __init__(self):
        self._environment = os.environ

    def ensure_env(self, env_name: str) -> str:
        env = self._environment.get(env_name)
        if not env:
            raise RuntimeError(
                f'Environment variable {env_name} is required for '
                f'service to work properly'
            )
        return env

    @property
    def environment(self):
        return self._environment

    def override_environment(self, environs: Mapping) -> None:
        self._environment.update(environs)

    def aws_region(self) -> str:
        """
        caas-api-handler, caas-event-handler to build envs for jobs.
        All the lambdas to init connections to clients.
        """
        return self._environment.get(CAASEnv.AWS_REGION)

    def system_customer(self) -> str | None:
        """
        Currently used only for event-driven scans in order to retrieve
        ED system rulesets.
        """
        return self._environment.get(CAASEnv.SYSTEM_CUSTOMER_NAME)

    def default_reports_bucket_name(self) -> str:
        """
        Lambdas:
        - caas-event-handler
        - caas-api-handler
        - caas-report-generator
        """
        return (self._environment.get(CAASEnv.REPORTS_BUCKET_NAME) or
                DEFAULT_REPORTS_BUCKET_NAME)

    def batch_job_log_level(self) -> str:
        """
        Lambdas:
        caas-api-handler
        caas-event-handler
        """
        return self._environment.get(CAASEnv.BATCH_JOB_LOG_LEVEL) or 'DEBUG'

    def get_batch_job_queue(self) -> str | None:
        """
        Lambdas:
        caas-api-handler
        caas-event-handler
        """
        return self._environment.get(CAASEnv.BATCH_JOB_QUEUE_NAME)

    def get_batch_job_def(self) -> str | None:
        """
        Lambdas:
        caas-api-handler
        caas-event-handler
        """
        return self._environment.get(CAASEnv.BATCH_JOB_DEF_NAME)

    def get_rulesets_bucket_name(self) -> str:
        """
        Lambdas:
        caas-api-handler
        caas-configuration-api-handler
        caas-configuration-updater
        caas-event-handler
        caas-license-updater
        caas-report-generator
        caas-ruleset-compiler
        """
        return (self._environment.get(CAASEnv.RULESETS_BUCKET_NAME) or
                DEFAULT_RULESETS_BUCKET_NAME)

    def get_metrics_bucket_name(self) -> str:
        """
        Lambdas:
        caas-metrics-updater
        caas-report-generator-handler
        """
        return (self._environment.get(CAASEnv.METRICS_BUCKET_NAME) or
                DEFAULT_METRICS_BUCKET_NAME)

    def get_user_pool_name(self) -> str | None:
        """
        Api lambdas:
        caas-api-handler
        caas-configuration-api-handler
        caas-report-generator
        """
        return self._environment.get(CAASEnv.USER_POOL_NAME)

    def get_user_pool_id(self) -> str | None:
        """
        It's optional but is preferred to use this instead of user_pool_name
        Api lambdas:
        caas-api-handler
        caas-configuration-api-handler
        caas-report-generator
        """
        return self._environment.get(CAASEnv.USER_POOL_ID)

    def get_job_lifetime_min(self) -> str:
        return (self._environment.get(CAASEnv.BATCH_JOB_LIFETIME_MINUTES) or
                '120')

    def get_statistics_bucket_name(self) -> str:
        return (self._environment.get(CAASEnv.STATISTICS_BUCKET_NAME) or
                DEFAULT_STATISTICS_BUCKET_NAME)

    def skip_cloud_identifier_validation(self) -> bool:
        """
        caas-api-handler
        """
        from_env = str(
            self._environment.get(CAASEnv.SKIP_CLOUD_IDENTIFIER_VALIDATION))
        return from_env.lower() in ENV_TRUE

    def is_docker(self) -> bool:
        return (self._environment.get(CAASEnv.SERVICE_MODE) ==
                DOCKER_SERVICE_MODE)

    def event_bridge_service_role(self) -> str | None:
        return self._environment.get(CAASEnv.EB_SERVICE_ROLE_TO_INVOKE_BATCH)

    def lambdas_alias_name(self) -> str | None:
        """
        To be able to trigger the valid lambda
        :return:
        """
        return self._environment.get(CAASEnv.LAMBDA_ALIAS_NAME)

    def account_id(self) -> str | None:
        # resolved from lambda context
        return self._environment.get(CAASEnv.ACCOUNT_ID)

    def is_testing(self) -> bool:
        return (str(self._environment.get(CAASEnv.TESTING_MODE)).lower() in
                ENV_TRUE)

    def mock_rabbitmq_s3_url(self) -> tuple[str, float] | None:
        data = self._environment.get(CAASEnv.MOCKED_RABBIT_MQ_S3)
        if not data:
            return
        url, rate = data.split(',')
        return url, float(rate)

    def jobs_time_to_live_days(self) -> int | None:
        """live_days
        Lambdas:
        - caas-api-handler
        """
        from_env = str(self._environment.get(CAASEnv.JOBS_TIME_TO_LIVE_DAYS))
        if from_env.isdigit():
            return int(from_env)
        return

    def events_ttl_hours(self) -> int | None:
        """
        Lambdas:
        - caas-api-handler
        """
        from_env = self._environment.get(CAASEnv.EVENTS_TTL_HOURS)
        if from_env:
            return int(from_env)
        return DEFAULT_EVENTS_TTL_HOURS

    def event_assembler_pull_item_limit(self) -> int:
        """
        Lambdas:
        - caas-event-handler
        """
        env = self._environment.get(
            CAASEnv.EVENT_ASSEMBLER_PULL_EVENTS_PAGE_SIZE)
        if env:
            return int(env)
        return 100

    def number_of_native_events_in_event_item(self) -> int:
        """
        Lambdas:
        - caas-api-handler
        """
        from_env = self._environment.get(CAASEnv.NATIVE_EVENTS_PER_ITEM)
        if from_env:
            return int(from_env)
        return DEFAULT_NUMBER_OF_EVENTS_IN_EVENT_ITEM

    def api_gateway_host(self) -> str | None:
        return self._environment.get(CAASEnv.API_GATEWAY_HOST)

    def api_gateway_stage(self) -> str | None:
        return self._environment.get(CAASEnv.API_GATEWAY_STAGE)

    def get_recommendation_bucket(self) -> str | None:
        return (self._environment.get(CAASEnv.RECOMMENDATIONS_BUCKET_NAME) or
                DEFAULT_RECOMMENDATION_BUCKET_NAME)

    def allow_simultaneous_jobs_for_one_tenant(self) -> bool:
        """
        caas-api-handler. Here we are talking about standard licensed
        jobs, not event-driven.
        :return:
        """
        return str(
            self._environment.get(
                CAASEnv.ALLOW_SIMULTANEOUS_JOBS_FOR_ONE_TENANT)
        ).lower() in ENV_TRUE

    def number_of_partitions_for_events(self) -> int:
        """
        https://aws.amazon.com/blogs/database/choosing-the-right-dynamodb-partition-key/
        We must be able to query CaaSEvents starting from some date. We
        cannot just scan the table, and also we cannot use one Partition key
        for all the events. So this setting defines the number of partitions.
        The more of them, the better will be writing throughput and harder read
        :return:
        """
        from_env = self._environment.get(
            CAASEnv.NUMBER_OF_PARTITIONS_FOR_EVENTS)
        if from_env:
            return int(from_env)
        return DEFAULT_NUMBER_OF_PARTITIONS_FOR_EVENTS

    def inner_cache_ttl_seconds(self) -> int:
        """
        Used for time to live cache
        :return:
        """
        from_env = str(self._environment.get(CAASEnv.INNER_CACHE_TTL_SECONDS))
        if from_env.isdigit():
            return int(from_env)
        return DEFAULT_INNER_CACHE_TTL_SECONDS

    def lm_token_lifetime_minutes(self):
        try:
            return int(self._environment.get(
                CAASEnv.LM_TOKEN_LIFETIME_MINUTES))
        except (TypeError, ValueError):
            return DEFAULT_LM_TOKEN_LIFETIME_MINUTES

    def allow_disabled_permissions(self) -> bool:
        env = str(self._environment.get(
            CAASEnv.ALLOW_DISABLED_PERMISSIONS_FOR_STANDARD_USERS
        ))
        return env.lower() in ENV_TRUE
