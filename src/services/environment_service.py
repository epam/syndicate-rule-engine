import os
import re
import tempfile
from typing import Optional

from helpers.constants import ENV_SERVICE_MODE, DOCKER_SERVICE_MODE, \
    ENV_VAR_REGION, TESTING_MODE_ENV, TESTING_MODE_ENV_TRUE, \
    ENV_VAR_JOBS_TIME_TO_LIVE_DAYS, ENV_NUMBER_OF_EVENTS_IN_EVENT_ITEM, \
    DEFAULT_NUMBER_OF_EVENTS_IN_EVENT_ITEM, ENV_VAR_EVENTS_TTL, \
    EVENT_STATISTICS_TYPE_VERBOSE, COMPONENT_NAME_ATTR, \
    EVENT_STATISTICS_TYPE_SHORT, ENV_EVENT_STATISTICS_TYPE, \
    AZURE_CLOUD_ATTR, ENV_API_GATEWAY_HOST, ENV_API_GATEWAY_STAGE, \
    AWS_CLOUD_ATTR, GOOGLE_CLOUD_ATTR, DEFAULT_STATISTICS_BUCKET_NAME, \
    DEFAULT_REPORTS_BUCKET_NAME, DEFAULT_RULESETS_BUCKET_NAME, \
    DEFAULT_SSM_BACKUP_BUCKET_NAME, DEFAULT_TEMPLATES_BUCKET_NAME, \
    DEFAULT_METRICS_BUCKET_NAME, ENV_ALLOW_SIMULTANEOUS_JOBS_FOR_ONE_TENANT, \
    DEFAULT_EVENTS_TTL_HOURS, ENV_VAR_NUMBER_OF_PARTITIONS_FOR_EVENTS, \
    DEFAULT_NUMBER_OF_PARTITIONS_FOR_EVENTS, \
    DEFAULT_RECOMMENDATION_BUCKET_NAME, DEFAULT_INNER_CACHE_TTL_SECONDS, \
    ENV_INNER_CACHE_TTL_SECONDS

ALLOWED_CLOUDS = {AWS_CLOUD_ATTR, AZURE_CLOUD_ATTR, GOOGLE_CLOUD_ATTR}

ENV_TRUE = {'1', 'true', 'yes', 'y'}


class EnvironmentService:

    def __init__(self):
        self._environment = os.environ

    def override_environment(self, environs: dict) -> None:
        self._environment.update(environs)

    def aws_region(self) -> str:
        """
        caas-api-handler, caas-event-handler to build envs for jobs.
        All the lambdas to init connections to clients.
        """
        return self._environment.get(ENV_VAR_REGION)

    def temp_folder_path(self):
        """
        This env var used to store temp files.
        Lambdas:
        caas-rule-meta-updater
        caas-ruleset-compiler
        """
        return self._environment.get(
            'TEMP_FOLDER_PATH') or tempfile.gettempdir()

    def default_reports_bucket_name(self):
        """
        Lambdas:
        - caas-event-handler
        - caas-api-handler
        - caas-report-generator
        """
        return self._environment.get('reports_bucket_name') or \
            DEFAULT_REPORTS_BUCKET_NAME

    def batch_job_log_level(self):
        """
        Lambdas:
        caas-api-handler
        caas-event-handler
        """
        return self._environment.get('batch_job_log_level') or 'DEBUG'

    def get_batch_job_queue(self):
        """
        Lambdas:
        caas-api-handler
        caas-event-handler
        """
        return self._environment.get('batch_job_queue_name')

    def get_batch_job_def(self):
        """
        Lambdas:
        caas-api-handler
        caas-event-handler
        """
        return self._environment.get('batch_job_def_name')

    def get_rulesets_bucket_name(self):
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
        return self._environment.get('caas_rulesets_bucket') or \
            DEFAULT_RULESETS_BUCKET_NAME

    def get_ssm_backup_bucket(self):
        """
        Lambdas:
        caas-configuration-updater
        caas-configuration-backupper
        """
        return self._environment.get('caas_ssm_backup_bucket') or \
            DEFAULT_SSM_BACKUP_BUCKET_NAME

    def get_ssm_backup_kms_key_id(self):
        """
        Lambdas:
        caas-configuration-updater
        caas-configuration-backupper
        """
        return self._environment.get('caas_ssm_backup_kms_key_id')

    def get_templates_bucket_name(self):
        """

        """
        return self._environment.get('templates_s3_bucket_name') or \
            DEFAULT_TEMPLATES_BUCKET_NAME

    def get_metrics_bucket_name(self) -> str:
        """
        Lambdas:
        caas-metrics-updater
        caas-report-generator-handler
        """
        return self._environment.get('metrics_bucket_name') or \
            DEFAULT_METRICS_BUCKET_NAME

    def get_user_pool_name(self):
        """
        Api lambdas:
        caas-api-handler
        caas-configuration-api-handler
        caas-report-generator
        """
        return self._environment.get('caas_user_pool_name')

    def get_user_pool_id(self):
        """
        It's optional but is preferred to use this instead of user_pool_name
        Api lambdas:
        caas-api-handler
        caas-configuration-api-handler
        caas-report-generator
        """
        return self._environment.get('caas_user_pool_id')

    def get_last_scan_threshold(self) -> int:
        """
        Threshold in seconds
        caas-api-handler
        """
        from_env = str(self._environment.get('last_scan_threshold'))
        if from_env.isdigit():
            return int(from_env)
        return 0

    def get_job_lifetime_min(self) -> str:
        return self._environment.get('job_lifetime_min') or '120'

    def get_statistics_bucket_name(self):
        return self._environment.get('stats_s3_bucket_name') or \
            DEFAULT_STATISTICS_BUCKET_NAME

    def allowed_clouds_to_scan(self) -> set:
        """
        Filter jobs for clouds.
        Clouds names must be separated by commas in env.
        """
        # "None"
        env = str(self._environment.get('feature_filter_jobs_request'))
        clouds = {cl.upper() for cl in env.split(',')} & ALLOWED_CLOUDS
        if not clouds:
            return ALLOWED_CLOUDS
        return clouds

    def get_image_folder_url(self):
        return self._environment.get('image_folder_url')

    def get_feature_update_ccc_version(self) -> bool:
        """
        caas-api-handler
        """
        return str(self._environment.get(
            'feature_update_ccc_version')).lower() in ENV_TRUE

    def get_feature_allow_only_temp_aws_credentials(self):
        value = str(
            self._environment.get('feature_allow_only_temp_aws_credentials'))
        return value.strip().lower() in ENV_TRUE

    def skip_cloud_identifier_validation(self) -> bool:
        """
        caas-api-handler
        """
        from_env = str(
            self._environment.get('feature_skip_cloud_identifier_validation'))
        return from_env.lower() in ENV_TRUE

    def is_docker(self) -> bool:
        return self._environment.get(ENV_SERVICE_MODE) == DOCKER_SERVICE_MODE

    def not_invoke_ruleset_compiler(self) -> bool:
        """In case we want to enable DynamoDB streams like it used to be"""
        from_env = self._environment.get('not_invoke_ruleset_compiler')
        if not from_env:
            return False
        return str(from_env).lower() in ENV_TRUE

    def event_bridge_service_role(self):
        return self._environment.get(
            'event_bridge_service_role_to_invoke_batch')

    def lambdas_alias_name(self) -> Optional[str]:
        """
        To be able to trigger the valid lambda
        :return:
        """
        return self._environment.get('lambdas_alias_name')

    def account_id(self) -> Optional[str]:
        maybe_id = self._environment.get('account_id') or ''
        res = re.search(r'\d{12}', maybe_id)
        return res.group() if res else None

    def is_testing(self) -> bool:
        return self._environment.get(TESTING_MODE_ENV) == TESTING_MODE_ENV_TRUE

    def jobs_time_to_live_days(self) -> Optional[int]:
        """
        Lambdas:
        - caas-api-handler
        """
        from_env = str(self._environment.get(ENV_VAR_JOBS_TIME_TO_LIVE_DAYS))
        if from_env.isdigit():
            return int(from_env)
        return

    def events_ttl_hours(self) -> Optional[int]:
        """
        Lambdas:
        - caas-api-handler
        """
        from_env = self._environment.get(ENV_VAR_EVENTS_TTL)
        if from_env:
            return int(from_env)
        return DEFAULT_EVENTS_TTL_HOURS

    def event_assembler_pull_item_limit(self):
        """
        Lambdas:
        - caas-event-handler
        """
        return self._environment.get('event_assembler_pull_item_limit') or 100

    def number_of_native_events_in_event_item(self) -> int:
        """
        Lambdas:
        - caas-api-handler
        """
        from_env = self._environment.get(ENV_NUMBER_OF_EVENTS_IN_EVENT_ITEM)
        if from_env:
            return int(from_env)
        return DEFAULT_NUMBER_OF_EVENTS_IN_EVENT_ITEM

    def event_statistics_type(self) -> str:
        """
        Lambdas:
        caas-event-handler
        """
        return self._environment.get(
            ENV_EVENT_STATISTICS_TYPE) or EVENT_STATISTICS_TYPE_VERBOSE

    def component_name(self) -> str:
        return self._environment.get(COMPONENT_NAME_ATTR)

    def is_event_statistics_verbose(self) -> bool:
        return self.event_statistics_type() == EVENT_STATISTICS_TYPE_SHORT

    def api_gateway_host(self) -> Optional[str]:
        return self._environment.get(ENV_API_GATEWAY_HOST)

    def api_gateway_stage(self) -> Optional[str]:
        return self._environment.get(ENV_API_GATEWAY_STAGE)

    def get_recommendation_bucket(self) -> Optional[str]:
        return self._environment.get('caas_recommendations_bucket') or \
            DEFAULT_RECOMMENDATION_BUCKET_NAME

    def allow_simultaneous_jobs_for_one_tenant(self) -> bool:
        """
        caas-api-handler. Here we are talking about standard licensed
        jobs, not event-driven.
        :return:
        """
        return str(
            self._environment.get(ENV_ALLOW_SIMULTANEOUS_JOBS_FOR_ONE_TENANT)
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
            ENV_VAR_NUMBER_OF_PARTITIONS_FOR_EVENTS)
        if from_env:
            return int(from_env)
        return DEFAULT_NUMBER_OF_PARTITIONS_FOR_EVENTS

    def inner_cache_ttl_seconds(self) -> int:
        """
        Used for time to live cache
        :return:
        """
        from_env = str(self._environment.get(ENV_INNER_CACHE_TTL_SECONDS))
        if from_env.isdigit():
            return int(from_env)
        return DEFAULT_INNER_CACHE_TTL_SECONDS
