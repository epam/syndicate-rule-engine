import operator
import os
from datetime import datetime
from enum import Enum
from itertools import filterfalse
from typing import Iterator, MutableMapping

from dateutil.relativedelta import SU, relativedelta
from typing_extensions import Self

# from http import HTTPMethod  # python3.11+


class HTTPMethod(str, Enum):
    HEAD = 'HEAD'
    GET = 'GET'
    POST = 'POST'
    PATCH = 'PATCH'
    DELETE = 'DELETE'
    PUT = 'PUT'


class CustodianEndpoint(str, Enum):
    """
    Should correspond to Api gateway models
    """

    DOC = '/doc'
    JOBS = '/jobs'
    ROLES = '/roles'
    RULES = '/rules'
    USERS = '/users'
    EVENT = '/event'
    SIGNIN = '/signin'
    SIGNUP = '/signup'
    HEALTH = '/health'
    REFRESH = '/refresh'
    TENANTS = '/tenants'
    RULESETS = '/rulesets'
    LICENSES = '/licenses'
    POLICIES = '/policies'
    JOBS_K8S = '/jobs/k8s'
    CUSTOMERS = '/customers'
    HEALTH_ID = '/health/{id}'
    JOBS_JOB = '/jobs/{job_id}'
    DOC_PROXY = '/doc/{proxy+}'
    ROLES_NAME = '/roles/{name}'
    CREDENTIALS = '/credentials'
    RULE_SOURCES = '/rule-sources'
    USERS_WHOAMI = '/users/whoami'
    SCHEDULED_JOB = '/scheduled-job'
    PLATFORMS_K8S = '/platforms/k8s'
    SETTINGS_MAIL = '/settings/mail'
    BATCH_RESULTS = '/batch-results'
    REPORTS_RETRY = '/reports/retry'
    POLICIES_NAME = '/policies/{name}'
    METRICS_STATUS = '/metrics/status'
    REPORTS_CLEVEL = '/reports/clevel'
    METRICS_UPDATE = '/metrics/update'
    REPORTS_STATUS = '/reports/status'
    REPORTS_PROJECT = '/reports/project'
    USERS_USERNAME = '/users/{username}'
    CREDENTIALS_ID = '/credentials/{id}'
    RULE_SOURCES_ID = '/rule-sources/{id}'
    RULESETS_RELEASE = '/rulesets/release'
    ED_RULESETS = '/rulesets/event-driven'
    DOC_SWAGGER_JSON = '/doc/swagger.json'
    RULE_META_UPDATER = '/rules/update-meta'
    REPORTS_PUSH_DOJO = '/reports/push/dojo'
    CUSTOMERS_RABBITMQ = '/customers/rabbitmq'
    REPORTS_DIAGNOSTIC = '/reports/diagnostic'
    REPORTS_DEPARTMENT = '/reports/department'
    INTEGRATIONS_SELF = '/integrations/temp/sre'
    SCHEDULED_JOB_NAME = '/scheduled-job/{name}'
    REPORTS_OPERATIONAL = '/reports/operational'
    TENANTS_TENANT_NAME = '/tenants/{tenant_name}'
    USERS_RESET_PASSWORD = '/users/reset-password'
    REPORTS_EVENT_DRIVEN = '/reports/event_driven'
    RULE_SOURCES_ID_SYNC = '/rule-sources/{id}/sync'
    LICENSES_LICENSE_KEY = '/licenses/{license_key}'
    SETTINGS_SEND_REPORTS = '/settings/send_reports'
    PLATFORMS_K8S_ID = '/platforms/k8s/{platform_id}'
    INTEGRATIONS_CHRONICLE = '/integrations/chronicle'
    CREDENTIALS_ID_BINDING = '/credentials/{id}/binding'
    CUSTOMERS_EXCLUDED_RULES = '/customers/excluded-rules'
    INTEGRATIONS_DEFECT_DOJO = '/integrations/defect-dojo'
    REPORTS_PUSH_DOJO_JOB_ID = '/reports/push/dojo/{job_id}'
    INTEGRATIONS_CHRONICLE_ID = '/integrations/chronicle/{id}'
    REPORTS_RULES_JOBS_JOB_ID = '/reports/rules/jobs/{job_id}'
    BATCH_RESULTS_JOB_ID = '/batch-results/{batch_results_id}'
    LICENSES_LICENSE_KEY_SYNC = '/licenses/{license_key}/sync'
    REPORTS_ERRORS_JOBS_JOB_ID = '/reports/errors/jobs/{job_id}'
    INTEGRATIONS_DEFECT_DOJO_ID = '/integrations/defect-dojo/{id}'
    REPORTS_DIGESTS_JOBS_JOB_ID = '/reports/digests/jobs/{job_id}'
    REPORTS_DETAILS_JOBS_JOB_ID = '/reports/details/jobs/{job_id}'
    TENANTS_TENANT_NAME_REGIONS = '/tenants/{tenant_name}/regions'
    REPORTS_FINDINGS_JOBS_JOB_ID = '/reports/findings/jobs/{job_id}'
    REPORTS_PUSH_CHRONICLE_JOB_ID = '/reports/push/chronicle/{job_id}'
    REPORTS_RESOURCES_JOBS_JOB_ID = '/reports/resources/jobs/{job_id}'
    REPORTS_COMPLIANCE_JOBS_JOB_ID = '/reports/compliance/jobs/{job_id}'
    SETTINGS_LICENSE_MANAGER_CLIENT = '/settings/license-manager/client'
    SETTINGS_LICENSE_MANAGER_CONFIG = '/settings/license-manager/config'
    LICENSE_LICENSE_KEY_ACTIVATION = '/licenses/{license_key}/activation'
    REPORTS_RULES_TENANTS_TENANT_NAME = '/reports/rules/tenants/{tenant_name}'
    TENANTS_TENANT_NAME_EXCLUDED_RULES = (
        '/tenants/{tenant_name}/excluded-rules'
    )
    TENANTS_TENANT_NAME_ACTIVE_LICENSES = (
        '/tenants/{tenant_name}/active-licenses'
    )
    INTEGRATIONS_CHRONICLE_ID_ACTIVATION = (
        '/integrations/chronicle/{id}/activation'
    )
    REPORTS_COMPLIANCE_TENANTS_TENANT_NAME = (
        '/reports/compliance/tenants/{tenant_name}'
    )
    INTEGRATIONS_DEFECT_DOJO_ID_ACTIVATION = (
        '/integrations/defect-dojo/{id}/activation'
    )
    REPORTS_DETAILS_TENANTS_TENANT_NAME_JOBS = (
        '/reports/details/tenants/{tenant_name}/jobs'
    )
    REPORTS_DIGESTS_TENANTS_TENANT_NAME_JOBS = (
        '/reports/digests/tenants/{tenant_name}/jobs'
    )
    REPORTS_FINDINGS_TENANTS_TENANT_NAME_JOBS = (
        '/reports/findings/tenants/{tenant_name}/jobs'
    )
    REPORTS_PUSH_CHRONICLE_TENANTS_TENANT_NAME = (
        '/reports/push/chronicle/tenants/{tenant_name}'
    )
    REPORTS_RESOURCES_TENANTS_TENANT_NAME_JOBS = (
        '/reports/resources/tenants/{tenant_name}/jobs'
    )
    REPORTS_RAW_TENANTS_TENANT_NAME_STATE_LATEST = (
        '/reports/raw/tenants/{tenant_name}/state/latest'
    )
    REPORTS_RESOURCES_TENANTS_TENANT_NAME_LATEST = (
        '/reports/resources/tenants/{tenant_name}/state/latest'
    )
    REPORTS_RESOURCES_PLATFORMS_K8S_PLATFORM_ID_LATEST = (
        '/reports/resources/platforms/k8s/{platform_id}/state/latest'
    )

    @classmethod
    def match(cls, resource: str) -> Self | None:
        """
        Tries to resolve endpoint from our enum from Api Gateway resource.
        Enum contains endpoints without stage. Though in general trailing
        slashes matter and endpoints with and without such slash are
        considered different we ignore this and consider such paths equal:
        - /path/to/resource
        - /path/to/resource/
        This method does the following:
        >>> CustodianEndpoint.match('/jobs/{job_id}') == CustodianEndpoint.JOBS_JOB
        >>> CustodianEndpoint.match('jobs/{job_id}') == CustodianEndpoint.JOBS_JOB
        >>> CustodianEndpoint.match('jobs/{job_id}/') == CustodianEndpoint.JOBS_JOB
        :param resource:
        :return:
        """
        raw = resource.strip('/')  # without trailing slashes
        for case in (raw, f'/{raw}', f'{raw}/', f'/{raw}/'):
            try:
                return cls(case)
            except ValueError:
                pass
        return


LAMBDA_URL_HEADER_CONTENT_TYPE_UPPER = 'Content-Type'
JSON_CONTENT_TYPE = 'application/json'

DEFAULT_SYSTEM_CUSTOMER: str = 'SYSTEM'
DEFAULT_RULES_METADATA_REPO_ACCESS_SSM_NAME = (
    'custodian.rules-metadata-repo-access'
)

ACTION_PARAM = 'action'

STANDARD = 'standard'

# Modular:Parent related attributes and types
CUSTODIAN_TYPE = 'CUSTODIAN'  # application that contains access to CUSTODIAN
SCHEDULED_JOB_TYPE = 'SCHEDULED_JOB'
META_ATTR = 'meta'
TENANT_ENTITY_TYPE = 'TENANT'
VALUE_ATTR = 'value'

TYPES_ATTR = 'types'
ID_ATTR = 'id'
CUSTOMER_ATTR = 'customer'
TENANT_ATTR = 'tenant'
TENANTS_ATTR = 'tenants'
TENANT_NAMES_ATTR = 'tenant_names'
ACCOUNT_ID_ATTR = 'account_id'
TENANT_DISPLAY_NAME_ATTR = 'tenant_display_name'
TENANT_DISPLAY_NAMES_ATTR = 'tenant_display_names'
TENANT_NAME_ATTR = 'tenant_name'
LATEST_LOGIN_ATTR = 'latest_login'
PRIMARY_CONTACTS_ATTR = 'primary_contacts'
SECONDARY_CONTACTS_ATTR = 'secondary_contacts'
TENANT_MANAGER_CONTACTS_ATTR = 'tenant_manager_contacts'
DEFAULT_OWNER_ATTR = 'default_owner'
CLOUD_ATTR = 'cloud'
CLOUD_IDENTIFIER_ATTR = 'cloud_identifier'
REGION_ATTR = 'region'
JOB_ID_ATTR = 'job_id'


class Cloud(str, Enum):
    """
    More like provider. "Cloud" is just a name that happen to be used
    """

    AWS = 'AWS'
    AZURE = 'AZURE'
    GOOGLE = 'GOOGLE'
    GCP = 'GOOGLE'  # alias
    K8S = 'KUBERNETES'  # alis
    KUBERNETES = 'KUBERNETES'

    @classmethod
    def parse(cls, cloud: str) -> Self | None:
        try:
            return cls[cloud.upper()]
        except KeyError:
            return


# The values of this enum represent what Custom core can scan, i.e. what
# type of rules and ruleset(s) we can have. These are not tenant clouds
class RuleDomain(str, Enum):
    AWS = 'AWS'
    AZURE = 'AZURE'
    GCP = 'GCP'
    GOOGLE = 'GCP'
    KUBERNETES = 'KUBERNETES'
    K8S = 'KUBERNETES'

    @classmethod
    def from_tenant_cloud(cls, cloud: str) -> Self | None:
        try:
            return cls[cloud.upper()]
        except KeyError:
            return


class JobType(str, Enum):
    MANUAL = 'manual'
    REACTIVE = 'reactive'


class ReportFormat(str, Enum):
    JSON = 'json'
    XLSX = 'xlsx'


class ReportDispatchStatus(str, Enum):
    FAILED = 'FAILED'
    SUCCEEDED = 'SUCCEEDED'
    DUPLICATE = 'DUPLICATE'
    RETRIED = 'RETRIED'
    PENDING = 'PENDING'


class PolicyErrorType(str, Enum):
    """
    For statistics
    """

    SKIPPED = 'SKIPPED'
    ACCESS = 'ACCESS'  # not enough permissions
    CREDENTIALS = 'CREDENTIALS'  # invalid credentials
    CLIENT = 'CLIENT'  # some other client error
    INTERNAL = 'INTERNAL'  # unexpected error


EXPIRATION_ATTR = 'expiration'

NAME_ATTR = 'name'
IMPACT_ATTR = 'impact'
RESOURCE_TYPE_ATTR = 'resource_type'
VERSION_ATTR = 'version'
FILTERS_ATTR = 'filters'
LOCATION_ATTR = 'location'
COMMENT_ATTR = 'comment'
LATEST_SYNC_ATTR = 'latest_sync'
COMMIT_HASH_ATTR = 'commit_hash'
COMMIT_TIME_ATTR = 'commit_time'
RULES_ATTR = 'rules'
RULESETS_ATTR = 'rulesets'
RULE_SOURCE_ID_ATTR = 'rule_source_id'
S3_PATH_ATTR = 's3_path'
RULES_NUMBER = 'rules_number'
STATUS_ATTR = 'status'
ALL_ATTR = 'all'
GIT_ACCESS_SECRET_ATTR = 'git_access_secret'
GIT_ACCESS_TYPE_ATTR = 'git_access_type'
GIT_PROJECT_ID_ATTR = 'git_project_id'
GIT_REF_ATTR = 'git_ref'
GIT_RULES_PREFIX_ATTR = 'git_rules_prefix'
GIT_URL_ATTR = 'git_url'


class RuleSourceSyncingStatus(str, Enum):
    SYNCING = 'SYNCING'
    SYNCED = 'SYNCED'
    FAILED = 'SYNCING_FAILED'


EVENT_DRIVEN_ATTR = 'event_driven'

DATA_ATTR = 'data'
ENABLED = 'enabled'
TRUSTED_ROLE_ARN = 'trusted_role_arn'

TYPE_ATTR = 'type'

RESTRICT_FROM_ATTR = 'restrict_from'
LICENSED_ATTR = 'licensed'
LICENSE_KEY_ATTR = 'license_key'
LICENSE_KEYS_ATTR = 'license_keys'
TENANT_LICENSE_KEY_ATTR = 'tenant_license_key'
TENANT_LICENSE_KEYS_ATTR = 'tenant_license_keys'
CUSTOMERS_ATTR = 'customers'

# License Manager[Setting].Config:
HOST_ATTR = 'host'
# License Manager[Setting].Client:

KID_ATTR = 'kid'
ALG_ATTR = 'alg'

TOKEN_ATTR = 'token'

PARAM_USER_ID = 'user_id'
PARAM_USER_SUB = 'user_sub'
PARAM_HTTP_METHOD = 'httpMethod'
PARAM_CUSTOMER = 'customer'
PARAM_USER_ROLE = 'user_role'
PARAM_USER_CUSTOMER = 'user_customer'

AUTHORIZATION_PARAM = 'authorization'

# on-prem
DOCKER_SERVICE_MODE, SAAS_SERVICE_MODE = 'docker', 'saas'

ENV_TRUE = {'1', 'true', 'yes', 'y'}

# RabbitMQ request
EXTERNAL_DATA_ATTR = 'externalData'
EXTERNAL_DATA_KEY_ATTR = 'externalDataKey'
EXTERNAL_DATA_BUCKET_ATTR = 'externalDataBucket'


_SENTINEL = object()


class EnvEnum(str, Enum):
    """
    Abstract enumeration class for holding environment variables
    """

    default: str | None

    @staticmethod
    def source() -> MutableMapping:
        return os.environ

    def __new__(cls, value: str, default: str | None = None):
        """
        All environment variables and optionally their default values.
        Since envs always have string type the default value also should be
        of string type and then converted to the necessary type in code.
        There is no default value if not specified (default equal to unset)
        """
        obj = str.__new__(cls, value)
        obj._value_ = value

        obj.default = default
        return obj

    def get(self, default=_SENTINEL) -> str | None:
        if default is _SENTINEL:
            default = self.default
        if default is not None:
            default = str(default)
        return self.source().get(self.value, default)

    def set(self, val: str | None):
        if val is None:
            self.source().pop(self.value, None)
        else:
            self.source()[self.value] = str(val)


class CAASEnv(EnvEnum):
    """
    Envs that can be set for lambdas of custodian service
    """

    SERVICE_MODE = 'CAAS_SERVICE_MODE'
    SYSTEM_CUSTOMER_NAME = 'SYSTEM_CUSTOMER_NAME', DEFAULT_SYSTEM_CUSTOMER
    LOG_LEVEL = 'CAAS_LOG_LEVEL', 'INFO'

    # inner envs (they are set automatically when request comes)
    API_GATEWAY_HOST = '_CAAS_API_GATEWAY_HOST'
    API_GATEWAY_STAGE = '_CAAS_API_GATEWAY_STAGE'
    INVOCATION_REQUEST_ID = '_INVOCATION_REQUEST_ID'

    # buckets
    RULESETS_BUCKET_NAME = 'CAAS_RULESETS_BUCKET_NAME', 'rulesets'
    REPORTS_BUCKET_NAME = 'CAAS_REPORTS_BUCKET_NAME', 'reports'
    METRICS_BUCKET_NAME = 'CAAS_METRICS_BUCKET_NAME', 'metrics'
    STATISTICS_BUCKET_NAME = 'CAAS_STATISTICS_BUCKET_NAME', 'statistics'
    RECOMMENDATIONS_BUCKET_NAME = (
        'CAAS_RECOMMENDATIONS_BUCKET_NAME',
        'recommendation',
    )

    # Cognito either one will work, but ID faster and safer
    USER_POOL_NAME = 'CAAS_USER_POOL_NAME'
    USER_POOL_ID = 'CAAS_USER_POOL_ID'

    # rbac
    ALLOW_DISABLED_PERMISSIONS_FOR_STANDARD_USERS = (
        'CAAS_ALLOW_DISABLED_PERMISSIONS_FOR_STANDARD_USERS'
    )

    # lm
    LM_TOKEN_LIFETIME_MINUTES = 'CAAS_LM_TOKEN_LIFETIME_MINUTES', '120'

    # some deployment options
    ACCOUNT_ID = 'CAAS_ACCOUNT_ID'
    LAMBDA_ALIAS_NAME = 'CAAS_LAMBDA_ALIAS_NAME'

    # batch options
    BATCH_JOB_DEF_NAME = 'CAAS_BATCH_JOB_DEF_NAME'
    BATCH_JOB_QUEUE_NAME = 'CAAS_BATCH_JOB_QUEUE_NAME'
    BATCH_JOB_LOG_LEVEL = 'CAAS_BATCH_JOB_LOG_LEVEL', 'DEBUG'
    BATCH_JOB_LIFETIME_MINUTES = 'CAAS_BATCH_JOB_LIFETIME_MINUTES', '180'
    EB_SERVICE_ROLE_TO_INVOKE_BATCH = 'CAAS_EB_SERVICE_ROLE_TO_INVOKE_BATCH'

    # events
    EVENTS_TTL_HOURS = 'CAAS_EVENTS_TTL_HOURS', '48'
    NATIVE_EVENTS_PER_ITEM = 'CAAS_NATIVE_EVENTS_PER_ITEM', '100'
    EVENT_ASSEMBLER_PULL_EVENTS_PAGE_SIZE = (
        'CAAS_EVENT_ASSEMBLER_PULL_EVENTS_PAGE_SIZE',
        '100',
    )
    NUMBER_OF_PARTITIONS_FOR_EVENTS = (
        'CAAS_NUMBER_OF_PARTITIONS_FOR_EVENTS',
        '10',
    )

    # jobs
    JOBS_TIME_TO_LIVE_DAYS = 'CAAS_JOBS_TIME_TO_LIVE_DAYS'

    # some logic setting
    SKIP_CLOUD_IDENTIFIER_VALIDATION = 'CAAS_SKIP_CLOUD_IDENTIFIER_VALIDATION'
    ALLOW_SIMULTANEOUS_JOBS_FOR_ONE_TENANT = (
        'CAAS_ALLOW_SIMULTANEOUS_JOBS_FOR_ONE_TENANT'
    )

    # cache
    INNER_CACHE_TTL_SECONDS = 'CAAS_INNER_CACHE_TTL_SECONDS', '300'

    # on-prem access
    MINIO_ENDPOINT = 'CAAS_MINIO_ENDPOINT'
    MINIO_ACCESS_KEY_ID = 'CAAS_MINIO_ACCESS_KEY_ID'
    MINIO_SECRET_ACCESS_KEY = 'CAAS_MINIO_SECRET_ACCESS_KEY'
    MINIO_PRESIGNED_URL_HOST = 'CAAS_MINIO_PRESIGNED_URL_HOST'

    VAULT_ENDPOINT = 'CAAS_VAULT_ENDPOINT'
    VAULT_TOKEN = 'CAAS_VAULT_TOKEN'

    MONGO_URI = 'CAAS_MONGO_URI'
    MONGO_DATABASE = 'CAAS_MONGO_DATABASE', 'custodian_as_a_service'

    AWS_REGION = 'AWS_REGION', 'us-east-1'

    # init envs
    SYSTEM_USER_PASSWORD = 'CAAS_SYSTEM_USER_PASSWORD'

    # Celery
    CELERY_BROKER_URL = 'CAAS_CELERY_BROKER_URL'


class BatchJobEnv(EnvEnum):
    """
    Batch executor specific envs. Note that batch can contain some envs from
    lambdas, but these that are listed here -> only for batch
    """

    # common
    AWS_REGION = 'AWS_REGION', 'us-east-1'
    BATCH_JOB_LIFETIME_MINUTES = 'CAAS_BATCH_JOB_LIFETIME_MINUTES', '120'
    SYSTEM_CUSTOMER_NAME = 'SYSTEM_CUSTOMER_NAME', DEFAULT_SYSTEM_CUSTOMER

    # specific to executor
    JOB_ID = 'AWS_BATCH_JOB_ID'
    CUSTODIAN_JOB_ID = 'CUSTODIAN_JOB_ID'
    BATCH_RESULTS_IDS = 'BATCH_RESULTS_IDS'

    TARGET_REGIONS = 'TARGET_REGIONS'
    AFFECTED_LICENSES = 'AFFECTED_LICENSES'

    EXECUTOR_MODE = 'EXECUTOR_MODE'
    JOB_TYPE = 'JOB_TYPE'
    SUBMITTED_AT = 'SUBMITTED_AT'

    AWS_DEFAULT_REGION = 'AWS_DEFAULT_REGION', 'us-east-1'
    CREDENTIALS_KEY = 'CREDENTIALS_KEY'

    SCHEDULED_JOB_NAME = 'SCHEDULED_JOB_NAME'
    TENANT_NAME = 'TENANT_NAME'
    PLATFORM_ID = 'PLATFORM_ID'
    ALLOW_MANAGEMENT_CREDS = 'ALLOW_MANAGEMENT_CREDENTIALS'


class Permission(str, Enum):
    is_disabled: bool
    depends_on_tenant: bool

    def __new__(
        cls,
        value: str,
        is_disabled: bool = False,
        depends_on_tenant: bool = False,
    ):
        """
        Hidden permissions are those that currently cannot be used by standard
        users even if the user has one. Those endpoints are available only for
        system user (because permissions are not checked if system user makes
        a request)
        :param value:
        :param is_disabled: is_disabled == allowed only for system
        :param depends_on_tenant: whether this permission can be allowed for
        one tenant and forbidden for another within one customer
        """
        obj = str.__new__(cls, value)
        obj._value_ = value

        obj.is_disabled = is_disabled
        obj.depends_on_tenant = depends_on_tenant
        return obj

    def __str__(self) -> str:
        return self.value

    # todo implement tenant restrictions where the True is commented
    REPORT_PUSH_TO_DOJO = 'report:push_report_to_dojo', False, True
    REPORT_PUSH_TO_DOJO_BATCH = 'report:push_to_dojo_batch', False  # True
    REPORT_PUSH_TO_CHRONICLE = 'report:push_report_to_chronicle', False, True
    REPORT_PUSH_TO_CHRONICLE_TENANT = (
        'report:push_report_to_chronicle_tenant',
        False,
        True,
    )
    REPORT_OPERATIONAL = 'report:post_operational', False, True
    REPORT_PROJECT = 'report:post_project', False  # True
    REPORT_DEPARTMENT = 'report:post_department', False  # True
    REPORT_CLEVEL = 'report:post_clevel'
    REPORT_DIAGNOSTIC = 'report:get_diagnostic'
    REPORT_STATUS = 'report:get_status'

    REPORT_DIGEST_DESCRIBE = 'report:get_digest', False, True
    REPORT_DIGEST_DESCRIBE_BATCH = 'report:get_digest_batch', False, True
    REPORT_DETAILS_DESCRIBE = 'report:get_details', False, True
    REPORT_DETAILS_DESCRIBE_BATCH = 'report:get_details_batch', False, True
    REPORT_FINDINGS_DESCRIBE = 'report:get_findings', False, True
    REPORT_FINDINGS_DESCRIBE_BATCH = 'report:get_findings_batch', False, True
    REPORT_COMPLIANCE_DESCRIBE_JOB = 'report:get_job_compliance', False, True
    REPORT_COMPLIANCE_DESCRIBE_TENANT = (
        'report:get_tenant_compliance',
        False,
        True,
    )
    REPORT_ERRORS_DESCRIBE = 'report:get_job_errors', False, True
    REPORT_RULES_DESCRIBE_JOB = 'report:get_job_rules', False, True
    REPORT_RULES_DESCRIBE_TENANT = 'report:get_tenant_rules', False, True
    REPORT_RESOURCES_GET_TENANT_LATEST = (
        'report:get_tenant_latest_resources',
        False,
        True,
    )
    REPORT_RESOURCES_GET_K8S_PLATFORM_LATEST = (
        'report:get_k8s_platform_latest_resources',
        False,
        True,
    )
    REPORT_RESOURCES_GET_JOBS = 'report:get_job_resources', False, True
    REPORT_RESOURCES_GET_JOBS_BATCH = (
        'report:get_job_resources_batch',
        False,
        True,
    )
    REPORT_RAW_GET_TENANT_LATEST = (
        'report:get_tenant_latest_raw_report',
        False,
        True,
    )

    JOB_QUERY = 'job:query', False  # True
    JOB_GET = 'job:get', False, True
    JOB_POST_LICENSED = 'job:post_for_tenant', False, True
    JOB_POST_K8S = 'job:post_for_k8s_platform', False, True
    JOB_TERMINATE = 'job:terminate', False, True

    CUSTOMER_DESCRIBE = 'customer:describe'
    CUSTOMER_SET_EXCLUDED_RULES = 'customer:set_excluded_rules'
    CUSTOMER_GET_EXCLUDED_RULES = 'customer:get_excluded_rules'

    TENANT_QUERY = 'tenant:query', False  # True
    TENANT_GET = 'tenant:get', False, True
    TENANT_GET_ACTIVE_LICENSES = 'tenant:get_active_licenses', False, True
    TENANT_SET_EXCLUDED_RULES = 'tenant:set_excluded_rules', False, True
    TENANT_GET_EXCLUDED_RULES = 'tenant:get_excluded_rules', False, True

    POLICY_DESCRIBE = 'iam:describe_policy'
    POLICY_CREATE = 'iam:create_policy'
    POLICY_UPDATE = 'iam:update_policy'
    POLICY_DELETE = 'iam:remove_policy'

    ROLE_DESCRIBE = 'iam:describe_role'
    ROLE_CREATE = 'iam:create_role'
    ROLE_UPDATE = 'iam:update_role'
    ROLE_DELETE = 'iam:remove_role'

    RULE_DESCRIBE = 'rule:describe'
    RULE_DELETE = 'rule:delete'
    RULE_UPDATE_META = 'system:update_meta'

    METRICS_UPDATE = 'system:update_metrics', True
    METRICS_STATUS = 'system:metrics_status'

    RULESET_DESCRIBE = 'ruleset:describe', False  # True
    RULESET_CREATE = 'ruleset:create'
    RULESET_UPDATE = 'ruleset:update'
    RULESET_DELETE = 'ruleset:delete'
    RULESET_DESCRIBE_ED = 'ruleset:describe_event_driven', True
    RULESET_CREATE_ED = 'ruleset:create_event_driven', True
    RULESET_DELETE_ED = 'ruleset:delete_event_driven', True
    RULESET_RELEASE = 'ruleset:release', False

    RULE_SOURCE_DESCRIBE = 'rule_source:describe'
    RULE_SOURCE_CREATE = 'rule_source:create'
    RULE_SOURCE_UPDATE = 'rule_source:update'
    RULE_SOURCE_DELETE = 'rule_source:delete'
    RULE_SOURCE_SYNC = 'rule_source:sync'

    EVENT_POST = 'event:post'

    LICENSE_ADD = 'license:add_license'
    LICENSE_QUERY = 'license:query', False  # True
    LICENSE_GET = 'license:get', False  # True
    LICENSE_DELETE = 'license:delete_license', False  # True
    LICENSE_SYNC = 'license:sync', True
    LICENSE_ACTIVATE = 'license:activate'
    LICENSE_GET_ACTIVATION = 'license:get_activation', False  # True
    LICENSE_DELETE_ACTIVATION = 'license:delete_activation', False
    LICENSE_UPDATE_ACTIVATION = 'license:update_activation', False

    SCHEDULED_JOB_GET = 'scheduled-job:get', False  # True
    SCHEDULED_JOB_QUERY = 'scheduled-job:query', False  # True
    SCHEDULED_JOB_CREATE = 'scheduled-job:register', False, True
    SCHEDULED_JOB_DELETE = 'scheduled-job:deregister', False  # True
    SCHEDULED_JOB_UPDATE = 'scheduled-job:update', False  # True

    SETTINGS_DESCRIBE_MAIL = 'settings:describe_mail', True
    SETTINGS_CREATE_MAIL = 'settings:create_mail', True
    SETTINGS_DELETE_MAIL = 'settings:delete_mail', True
    SETTINGS_CHANGE_SET_REPORTS = (
        'settings:change_send_reports',
        True,
    )  # TODO make PUT
    SETTINGS_DESCRIBE_LM_CONFIG = 'settings:describe_lm_config'
    SETTINGS_CREATE_LM_CONFIG = 'settings:create_lm_config', True
    SETTINGS_DELETE_LM_CONFIG = 'settings:delete_lm_config', True
    SETTINGS_DESCRIBE_LM_CLIENT = 'settings:describe_lm_client'
    SETTINGS_CREATE_LM_CLIENT = 'settings:create_lm_client', True
    SETTINGS_DELETE_LM_CLIENT = 'settings:delete_lm_client', True

    RABBITMQ_DESCRIBE = 'rabbitmq:describe'
    RABBITMQ_CREATE = 'rabbitmq:create'
    RABBITMQ_DELETE = 'rabbitmq:delete'

    BATCH_RESULTS_GET = 'batch_results:get', False, True
    BATCH_RESULTS_QUERY = 'batch_results:query', False  # True

    PLATFORM_GET_K8S = 'platform:get_k8s', False, True
    PLATFORM_QUERY_K8S = 'platform:query_k8', False  # True
    PLATFORM_CREATE_K8S = 'platform:create_k8s', False, True
    PLATFORM_DELETE_K8S = 'platform:delete_k8s', False, True

    SRE_INTEGRATION_CREATE = 'self_integration:create'
    SRE_INTEGRATION_UPDATE = 'self_integration:update'
    SRE_INTEGRATION_DESCRIBE = 'self_integration:describe'
    SRE_INTEGRATION_DELETE = 'self_integration:delete'

    DOJO_INTEGRATION_CREATE = 'dojo_integration:create'
    DOJO_INTEGRATION_DESCRIBE = 'dojo_integration:describe'
    DOJO_INTEGRATION_DELETE = 'dojo_integration:delete'
    DOJO_INTEGRATION_ACTIVATE = 'dojo_integration:activate'
    DOJO_INTEGRATION_GET_ACTIVATION = 'dojo_integration:get_activation'
    DOJO_INTEGRATION_DELETE_ACTIVATION = 'dojo_integration:delete_activation'

    CHRONICLE_INTEGRATION_CREATE = 'chronicle_integration:create'
    CHRONICLE_INTEGRATION_DESCRIBE = 'chronicle_integration:describe'
    CHRONICLE_INTEGRATION_DELETE = 'chronicle_integration:delete'
    CHRONICLE_INTEGRATION_ACTIVATE = 'chronicle_integration:activate'
    CHRONICLE_INTEGRATION_GET_ACTIVATION = (
        'chronicle_integration:get_activation'
    )
    CHRONICLE_INTEGRATION_DELETE_ACTIVATION = (
        'chronicle_integration:delete_activation'
    )

    CREDENTIALS_DESCRIBE = 'credentials:describe'
    CREDENTIALS_BIND = 'credentials:bind'
    CREDENTIALS_UNBIND = 'credentials:unbind'
    CREDENTIALS_GET_BINDING = 'credentials:get_binding'

    USERS_DESCRIBE = 'users:describe'
    USERS_CREATE = 'users:create'
    USERS_UPDATE = 'users:update'
    USERS_DELETE = 'users:delete'
    USERS_GET_CALLER = 'users:get_caller'
    USERS_RESET_PASSWORD = 'users:reset_password'

    @classmethod
    def iter_enabled(cls) -> Iterator[Self]:
        """
        Iterates over all the currently available permission
        :return:
        """
        return filterfalse(operator.attrgetter('is_disabled'), cls)

    @classmethod
    def iter_disabled(cls) -> Iterator[Self]:
        return filter(operator.attrgetter('is_disabled'), cls)


class PolicyEffect(str, Enum):
    ALLOW = 'allow'
    DENY = 'deny'


# Modular
# Tenant
MODULAR_MANAGEMENT_ID_ATTR = 'management_parent_id'
MODULAR_CLOUD_ATTR = CLOUD_ATTR
MODULAR_DISPLAY_NAME_ATTR = 'display_name'
MODULAR_READ_ONLY_ATTR = 'read_only'
MODULAR_DISPLAY_NAME_TO_LOWER = 'display_name_to_lower'
MODULAR_CONTACTS = 'contacts'
MODULAR_PARENT_MAP = 'parent_map'
# Application
MODULAR_IS_DELETED = 'is_deleted'
MODULAR_DELETION_DATE = 'deletion_date'
MODULAR_SECRET = 'secret'
MODULAR_TYPE = 'type'


class BatchJobType(str, Enum):
    """
    Our inner types
    """

    STANDARD = 'standard'
    EVENT_DRIVEN = 'event-driven-multi-account'
    SCHEDULED = 'scheduled'


# event-driven
AWS_VENDOR = 'AWS'
MAESTRO_VENDOR = 'MAESTRO'

# smtp
PASSWORD_ATTR = 'password'

# reports
DATA_TYPE = 'data_type'
TOTAL_SCANS_ATTR = 'total_scans'
FAILED_SCANS_ATTR = 'failed_scans'
SUCCEEDED_SCANS_ATTR = 'succeeded_scans'
COMPLIANCE_TYPE = 'compliance'
RULE_TYPE = 'rule'
OVERVIEW_TYPE = 'overview'
RESOURCES_TYPE = 'resources'
ATTACK_VECTOR_TYPE = 'attack_vector'
FINOPS_TYPE = 'finops'
KUBERNETES_TYPE = 'kubernetes'
LAST_SCAN_DATE = 'last_scan_date'
RESOURCE_TYPES_DATA_ATTR = 'resource_types_data'
SEVERITY_DATA_ATTR = 'severity_data'
ACTIVATED_REGIONS_ATTR = 'activated_regions'
AVERAGE_DATA_ATTR = 'average_data'
END_DATE = 'end_date'
OUTDATED_TENANTS = 'outdated_tenants'
ARCHIVE_PREFIX = 'archive'


class JobState(str, Enum):
    """
    https://docs.aws.amazon.com/batch/latest/userguide/job_states.html
    """

    SUBMITTED = 'SUBMITTED'
    PENDING = 'PENDING'
    RUNNABLE = 'RUNNABLE'
    STARTING = 'STARTING'
    RUNNING = 'RUNNING'
    FAILED = 'FAILED'
    SUCCEEDED = 'SUCCEEDED'


# Maestro Credentials Applications types
AZURE_CREDENTIALS_APP_TYPE = 'AZURE_CREDENTIALS'
AZURE_CERTIFICATE_APP_TYPE = 'AZURE_CERTIFICATE'
AWS_CREDENTIALS_APP_TYPE = 'AWS_CREDENTIALS'
AWS_ROLE_APP_TYPE = 'AWS_ROLE'
GCP_COMPUTE_ACCOUNT_APP_TYPE = 'GCP_COMPUTE_ACCOUNT'
GCP_SERVICE_ACCOUNT_APP_TYPE = 'GCP_SERVICE_ACCOUNT'

GLOBAL_REGION = 'global'


class HealthCheckStatus(str, Enum):
    OK = 'OK'
    UNKNOWN = 'UNKNOWN'
    NOT_OK = 'NOT_OK'


# cognito
COGNITO_USERNAME = 'cognito:username'
COGNITO_SUB = 'sub'
CUSTOM_ROLE_ATTR = 'custom:role'
CUSTOM_CUSTOMER_ATTR = 'custom:customer'
CUSTOM_LATEST_LOGIN_ATTR = 'custom:latest_login'
CUSTOM_TENANTS_ATTR = 'custom:tenants'

TACTICS_ID_MAPPING = {  # rules do not have tactic IDs
    'Reconnaissance': 'TA0043',
    'Resource Development': 'TA0042',
    'Initial Access': 'TA0001',
    'Execution': 'TA0002',
    'Persistence': 'TA0003',
    'Privilege Escalation': 'TA0004',
    'Defense Evasion': 'TA0005',
    'Credential Access': 'TA0006',
    'Discovery': 'TA0007',
    'Lateral Movement': 'TA0008',
    'Collection': 'TA0009',
    'Exfiltration': 'TA0010',
    'Command and Control': 'TA0011',
    'Impact': 'TA0040',
}

RETRY_REPORT_STATE_MACHINE = 'retry_send_reports'
SEND_REPORTS_STATE_MACHINE = 'send_reports'

START_DATE = 'start_date'
ARTICLE_ATTR = 'article'

COMPOUND_KEYS_SEPARATOR = '#'

ED_AWS_RULESET_NAME = '_ED_AWS'
ED_AZURE_RULESET_NAME = '_ED_AZURE'
ED_GOOGLE_RULESET_NAME = '_ED_GOOGLE'
ED_KUBERNETES_RULESET_NAME = '_ED_KUBERNETES'


class RuleSourceType(str, Enum):
    GITHUB = 'GITHUB'
    GITLAB = 'GITLAB'
    GITHUB_RELEASE = 'GITHUB_RELEASE'  # means that rules from the latest release will be used


class S3SettingKey(str, Enum):
    RULES_TO_SEVERITY = 'RULES_TO_SEVERITY'
    RULES_TO_SERVICE_SECTION = 'RULES_TO_SERVICE_SECTION'
    RULES_TO_STANDARDS = 'RULES_TO_STANDARDS'
    RULES_TO_SERVICE = 'RULES_TO_SERVICE'
    RULES_TO_MITRE = 'RULES_TO_MITRE'
    RULES_TO_CATEGORY = 'RULES_TO_CATEGORY'
    HUMAN_DATA = 'HUMAN_DATA'
    CLOUD_TO_RULES = 'CLOUD_TO_RULES'

    AWS_STANDARDS_COVERAGE = 'AWS_STANDARDS_COVERAGE'
    AZURE_STANDARDS_COVERAGE = 'AZURE_STANDARDS_COVERAGE'
    GOOGLE_STANDARDS_COVERAGE = 'GOOGLE_STANDARDS_COVERAGE'

    AWS_EVENTS = 'AWS_EVENTS'
    AZURE_EVENTS = 'AZURE_EVENTS'
    GOOGLE_EVENTS = 'GOOGLE_EVENTS'

    EVENT_BRIDGE_EVENT_SOURCE_TO_RULES_MAPPING = (
        'EVENT_BRIDGE_EVENT_SOURCE_TO_RULES_MAPPING'
    )
    MAESTRO_SUBGROUP_ACTION_TO_AZURE_EVENTS_MAPPING = (
        'MAESTRO_SUBGROUP_ACTION_TO_AZURE_EVENTS_MAPPING'
    )
    MAESTRO_SUBGROUP_ACTION_TO_GOOGLE_EVENTS_MAPPING = (
        'MAESTRO_SUBGROUP_ACTION_TO_GOOGLE_EVENTS_MAPPING'
    )


class SettingKey(str, Enum):
    MAIL_CONFIGURATION = 'MAIL_CONFIGURATION'
    LM_CLIENT_KEY = 'LM_CLIENT_KEY'
    ACCESS_DATA_LM = 'ACCESS_DATA_LM'
    TEMPLATE_BUCKET = 'TEMPLATES_S3_BUCKET_NAME'
    SYSTEM_CUSTOMER = 'SYSTEM_CUSTOMER_NAME'
    EVENT_ASSEMBLER = 'EVENT_ASSEMBLER'
    REPORT_DATE_MARKER = 'REPORT_DATE_MARKER'
    RULES_METADATA_REPO_ACCESS_SSM_NAME = 'RULES_METADATA_REPO_ACCESS_SSM_NAME'

    AWS_STANDARDS_COVERAGE = 'AWS_STANDARDS_COVERAGE'
    AZURE_STANDARDS_COVERAGE = 'AZURE_STANDARDS_COVERAGE'
    GOOGLE_STANDARDS_COVERAGE = 'GOOGLE_STANDARDS_COVERAGE'

    SEND_REPORTS = 'SEND_REPORTS'
    MAX_ATTEMPT = 'MAX_ATTEMPT'
    MAX_CRON_NUMBER = 'MAX_CRON_NUMBER'
    MAX_RABBITMQ_REQUEST_SIZE = 'MAX_RABBITMQ_REQUEST_SIZE'


class PlatformType(str, Enum):
    SELF_MANAGED = 'SELF_MANAGED'  # any
    EKS = 'EKS'  # aws
    AKS = 'AKS'  # azure
    GKS = 'GKS'  # google


class Severity(str, Enum):
    """
    Low to High
    """

    INFO = 'Info'
    LOW = 'Low'
    MEDIUM = 'Medium'
    HIGH = 'High'
    UNKNOWN = 'Unknown'

    @classmethod
    def iter(cls):
        return map(operator.attrgetter('value'), cls)

    @classmethod
    def parse(cls, sev: str | None, /) -> 'Severity':
        if not sev:
            return cls.UNKNOWN
        try:
            return cls(sev.strip().capitalize())
        except ValueError:
            return cls.UNKNOWN


class RemediationComplexity(str, Enum):
    UNKNOWN = 'Unknown'
    LOW = 'Low'
    LOW_MEDIUM = 'Low-Medium'
    MEDIUM = 'Medium'
    MEDIUM_HIGH = 'Medium-High'
    HIGH = 'High'

    @classmethod
    def parse(cls, rem: str | None, /) -> 'RemediationComplexity':
        if not rem:
            return cls.UNKNOWN
        try:
            return cls(rem.strip().title())
        except ValueError:
            return cls.UNKNOWN


REPORT_FIELDS = {
    'id',
    'name',
    'arn',  # aws specific
    'namespace',  # k8s specific
}  # from Cloud Custodian

PRIVATE_KEY_SECRET_NAME = 'rule-engine-private-key'

# tenant setting keys
TS_EXCLUDED_RULES_KEY = 'CUSTODIAN_EXCLUDED_RULES'
TS_JOB_LOCK_KEY = 'CUSTODIAN_JOB_LOCK'

GITHUB_API_URL_DEFAULT = 'https://api.github.com'
GITLAB_API_URL_DEFAULT = 'https://git.epam.com'

# lambda names
RULE_META_UPDATER_LAMBDA_NAME = 'caas-rule-meta-updater'
METRICS_UPDATER_LAMBDA_NAME = 'caas-metrics-updater'


# some common deltas for reports
_last_sunday = relativedelta(
    hour=0, minute=0, second=0, microsecond=0, weekday=SU(-1)
)

_previous_month_start = relativedelta(
    hour=0, minute=0, second=0, microsecond=0, months=-1, day=1
)
_this_month_start = relativedelta(
    hour=0, minute=0, second=0, microsecond=0, day=1
)


class ReportType(str, Enum):
    """
    Each member of the enum represents a specific type of report that can
    be generated and sent (currently they could be sent only by using Maestro).
    Each member holds a value of enum, a description that is not used
    externally but only for developers and two time deltas: relative start
    date and a relative end date. These dates represent the reporting period
    for that concrete type relatively to now.
    If start date is omitted it means that either start date is not important
    for this type of report (for example report as of specific date) or
    the report needs all historical data available without lower bound. It
    depends on report type but currently there are no the latters.
    Also, the principal entity for any report depends on report type.

    Currently, it can be described as:
    - Operational = one tenant scope;
    - Project = one tenant group scope (multiple tenants with different
      clouds that belong to the same Maestro project);
    - Department = one customer scope
    - Clevel = one customer scope
    """

    description: str
    r_end: relativedelta  # relatively to now
    r_start: relativedelta | None

    def __new__(
        cls,
        value: str,
        description: str,
        r_start: relativedelta | None = None,
        r_end: relativedelta | None = None,
    ):
        obj = str.__new__(cls, value)
        obj._value_ = value

        obj.description = description
        obj.r_end = r_end or relativedelta()
        obj.r_start = r_start
        return obj

    def end(self, now: datetime) -> datetime:
        """
        This data
        """
        return now + self.r_end

    def start(self, now: datetime) -> datetime | None:
        if not self.r_start:
            return
        return now + self.r_start

    # Operational, kind of for one tenant
    OPERATIONAL_OVERVIEW = (
        'OPERATIONAL_OVERVIEW',
        'Data for a specific tenant of any cloud for a week period from Sunday till Sunday. Contains number of different jobs, total number of resources by region and by severities',
        _last_sunday,
    )
    OPERATIONAL_RESOURCES = (
        'OPERATIONAL_RESOURCES',
        'All resources for a specific tenant as of date of generation',
        _last_sunday,
    )
    OPERATIONAL_RULES = (
        'OPERATIONAL_RULES',
        'Average rules usage statistics for tenant within this week',
        _last_sunday,
    )
    OPERATIONAL_COMPLIANCE = (
        'OPERATIONAL_COMPLIANCE',
        'Compliance per tenant as of date of generation',
        _last_sunday,
    )
    OPERATIONAL_FINOPS = (
        'OPERATIONAL_FINOPS',
        'Finops report per tenant as of date of generation',
        _last_sunday,
    )
    OPERATIONAL_ATTACKS = (
        'OPERATIONAL_ATTACKS',
        'MITRE Attacks report per tenant as of date of generation',
        _last_sunday,
    )
    OPERATIONAL_KUBERNETES = (
        'OPERATIONAL_KUBERNETES',
        'Just old K8S report as of date of generation. It contains both MITRE and Resources data',
        _last_sunday,
    )
    OPERATIONAL_DEPRECATION = (
        'OPERATIONAL_DEPRECATION',
        'Displays resources that will be soon deprecated',
        _last_sunday,
    )

    # Project, for a group of tenants within one project
    PROJECT_OVERVIEW = (
        'PROJECT_OVERVIEW',
        'Overview data per group of tenants',
        _last_sunday,
    )
    PROJECT_COMPLIANCE = (
        'PROJECT_COMPLIANCE',
        'Compliance data per group of tenants',
        _last_sunday,
    )
    PROJECT_RESOURCES = (
        'PROJECT_RESOURCES',
        'Resources data per group of tenants',
        _last_sunday,
    )
    PROJECT_ATTACKS = (
        'PROJECT_ATTACKS',
        'Attacks data per group of tenants',
        _last_sunday,
    )
    PROJECT_FINOPS = (
        'PROJECT_FINOPS',
        'Finops data per group of tenants',
        _last_sunday,
    )

    # Department, tops by tenants
    DEPARTMENT_TOP_RESOURCES_BY_CLOUD = (
        'DEPARTMENT_TOP_RESOURCES_BY_CLOUD',
        'Top resources in tenants by cloud',
        _previous_month_start,
        _this_month_start,
    )
    DEPARTMENT_TOP_TENANTS_RESOURCES = (
        'DEPARTMENT_TOP_TENANTS_RESOURCES',
        'As the name suggests',
        _previous_month_start,
        _this_month_start,
    )
    DEPARTMENT_TOP_TENANTS_COMPLIANCE = (
        'DEPARTMENT_TOP_TENANTS_COMPLIANCE',
        'As the name suggests',
        _previous_month_start,
        _this_month_start,
    )
    DEPARTMENT_TOP_COMPLIANCE_BY_CLOUD = (
        'DEPARTMENT_TOP_COMPLIANCE_BY_CLOUD',
        'As the name suggests',
        _previous_month_start,
        _this_month_start,
    )
    DEPARTMENT_TOP_TENANTS_ATTACKS = (
        'DEPARTMENT_TOP_TENANTS_ATTACKS',
        'As the name suggests',
        _previous_month_start,
        _this_month_start,
    )
    DEPARTMENT_TOP_ATTACK_BY_CLOUD = (
        'DEPARTMENT_TOP_ATTACK_BY_CLOUD',
        'As the name suggests',
        _previous_month_start,
        _this_month_start,
    )

    # C-Level, kind of for the whole customer
    C_LEVEL_OVERVIEW = (
        'C_LEVEL_OVERVIEW',
        'Data across all tenants within clouds for a previous month',
        _previous_month_start,
        _this_month_start,
    )
    C_LEVEL_COMPLIANCE = (
        'C_LEVEL_COMPLIANCE',
        'Standards coverage across all tenants within customer ...?',
        _previous_month_start,
        _this_month_start,
    )
    C_LEVEL_ATTACKS = (
        'C_LEVEL_ATTACKS',
        'Attacks across all tenants within customer ...?',
        _previous_month_start,
        _this_month_start,
    )


class RabbitCommand(str, Enum):
    SEND_MAIL = 'SEND_MAIL'
