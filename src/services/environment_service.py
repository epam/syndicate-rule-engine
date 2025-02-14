import os
from typing import Mapping

from helpers.constants import (
    CAASEnv,
    DOCKER_SERVICE_MODE,
    ENV_TRUE,
)


class EnvironmentService:
    def override_environment(self, environs: Mapping) -> None:
        os.environ.update(environs)

    def aws_region(self) -> str:
        """
        caas-api-handler, caas-event-handler to build envs for jobs.
        All the lambdas to init connections to clients.
        """
        return CAASEnv.AWS_REGION.get()

    def default_reports_bucket_name(self) -> str:
        """
        Lambdas:
        - caas-event-handler
        - caas-api-handler
        - caas-report-generator
        """
        return CAASEnv.REPORTS_BUCKET_NAME.get()

    def batch_job_log_level(self) -> str:
        """
        Lambdas:
        caas-api-handler
        caas-event-handler
        """
        return CAASEnv.BATCH_JOB_LOG_LEVEL.get()

    def get_batch_job_queue(self) -> str | None:
        """
        Lambdas:
        caas-api-handler
        caas-event-handler
        """
        return CAASEnv.BATCH_JOB_QUEUE_NAME.get()

    def get_batch_job_def(self) -> str | None:
        """
        Lambdas:
        caas-api-handler
        caas-event-handler
        """
        return CAASEnv.BATCH_JOB_DEF_NAME.get()

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
        return CAASEnv.RULESETS_BUCKET_NAME.get()

    def get_metrics_bucket_name(self) -> str:
        """
        Lambdas:
        caas-metrics-updater
        caas-report-generator-handler
        """
        return CAASEnv.METRICS_BUCKET_NAME.get()

    def get_user_pool_name(self) -> str | None:
        """
        Api lambdas:
        caas-api-handler
        caas-configuration-api-handler
        caas-report-generator
        """
        return CAASEnv.USER_POOL_NAME.get()

    def get_user_pool_id(self) -> str | None:
        """
        It's optional but is preferred to use this instead of user_pool_name
        Api lambdas:
        caas-api-handler
        caas-configuration-api-handler
        caas-report-generator
        """
        return CAASEnv.USER_POOL_ID.get()

    def get_job_lifetime_min(self) -> str:
        return CAASEnv.BATCH_JOB_LIFETIME_MINUTES.get()

    def get_statistics_bucket_name(self) -> str:
        return CAASEnv.STATISTICS_BUCKET_NAME.get()

    def skip_cloud_identifier_validation(self) -> bool:
        """
        caas-api-handler
        """
        return CAASEnv.SKIP_CLOUD_IDENTIFIER_VALIDATION.get('').lower() in ENV_TRUE

    def is_docker(self) -> bool:
        return CAASEnv.SERVICE_MODE.get() == DOCKER_SERVICE_MODE

    def event_bridge_service_role(self) -> str | None:
        return CAASEnv.EB_SERVICE_ROLE_TO_INVOKE_BATCH.get()

    def lambdas_alias_name(self) -> str | None:
        """
        To be able to trigger the valid lambda
        :return:
        """
        return CAASEnv.LAMBDA_ALIAS_NAME.get()

    def account_id(self) -> str | None:
        # resolved from lambda context
        return CAASEnv.ACCOUNT_ID.get()

    def jobs_time_to_live_days(self) -> int | None:
        """live_days
        Lambdas:
        - caas-api-handler
        """
        from_env = CAASEnv.JOBS_TIME_TO_LIVE_DAYS.get('')
        if from_env.isdigit():
            return int(from_env)
        return

    def events_ttl_hours(self) -> int | None:
        """
        Lambdas:
        - caas-api-handler
        """
        return int(CAASEnv.EVENTS_TTL_HOURS.get())

    def event_assembler_pull_item_limit(self) -> int:
        """
        Lambdas:
        - caas-event-handler
        """
        return int(CAASEnv.EVENT_ASSEMBLER_PULL_EVENTS_PAGE_SIZE.get())

    def number_of_native_events_in_event_item(self) -> int:
        """
        Lambdas:
        - caas-api-handler
        """
        return int(CAASEnv.NATIVE_EVENTS_PER_ITEM.get())

    def api_gateway_host(self) -> str | None:
        return CAASEnv.API_GATEWAY_HOST.get()

    def api_gateway_stage(self) -> str | None:
        return CAASEnv.API_GATEWAY_STAGE.get()

    def get_recommendation_bucket(self) -> str | None:
        return CAASEnv.RECOMMENDATIONS_BUCKET_NAME.get()

    def allow_simultaneous_jobs_for_one_tenant(self) -> bool:
        """
        caas-api-handler. Here we are talking about standard licensed
        jobs, not event-driven.
        :return:
        """
        return CAASEnv.ALLOW_SIMULTANEOUS_JOBS_FOR_ONE_TENANT.get('').lower() in ENV_TRUE

    def number_of_partitions_for_events(self) -> int:
        """
        https://aws.amazon.com/blogs/database/choosing-the-right-dynamodb-partition-key/
        We must be able to query CaaSEvents starting from some date. We
        cannot just scan the table, and also we cannot use one Partition key
        for all the events. So this setting defines the number of partitions.
        The more of them, the better will be writing throughput and harder read
        :return:
        """
        return int(CAASEnv.NUMBER_OF_PARTITIONS_FOR_EVENTS.get())

    def lm_token_lifetime_minutes(self) -> int:
        return int(CAASEnv.LM_TOKEN_LIFETIME_MINUTES.get())

    def allow_disabled_permissions(self) -> bool:
        return CAASEnv.ALLOW_DISABLED_PERMISSIONS_FOR_STANDARD_USERS.get('').lower() in ENV_TRUE

    def minio_presigned_url_host(self) -> str | None:
        host = CAASEnv.MINIO_PRESIGNED_URL_HOST.get()
        if host:
            return host.strip().strip('/')
