PARAM_CUSTOMER_ID = 'customer_id'
PARAM_TENANT_ID = 'tenant_id'
PARAM_NAME = 'name'
PARAM_PERMISSIONS = 'permissions'
PARAM_PERMISSIONS_ADMIN = 'permissions_admin'
PARAM_EXPIRATION = 'expiration'
PARAM_POLICIES = 'policies'
PARAM_DISPLAY_NAME = 'display_name'
PARAM_CREDENTIALS = 'credentials'
PARAM_ACCESS_TOKEN = 'access_token'  # google
PARAM_PROJECT_ID = 'project_id'  # google
PARAM_OWNER = 'owner'
PARAM_INHERIT = 'inherit'
PARAM_CLOUD_IDENTIFIER = 'cloud_identifier'
PARAM_RULES_TO_EXCLUDE = 'rules_to_exclude'
PARAM_RULES_TO_INCLUDE = 'rules_to_include'
PARAM_ENABLED = 'enabled'
PARAM_TRUSTED_ROLE_ARN = 'trusted_role_arn'
PARAM_VERSION = 'version'
PARAM_CLOUD = 'cloud'
PARAM_RULES = 'rules'
PARAM_GET_RULES = 'get_rules'
PARAM_STANDARD = 'standard'
PARAM_RULES_NUMBER = 'rules_number'
PARAM_REGION_STATE = 'state'
PARAM_ALL_REGIONS = 'all_regions'
PARAM_CUSTOMER = 'customer'
PARAM_TENANT = 'tenant'
PARAM_TENANTS = 'tenants'
PARAM_ACCOUNT = 'account'
PARAM_ACCOUNTS = 'accounts'
PARAM_JOB_ID = 'job_id'
PARAM_TARGET_RULESETS = 'target_rulesets'
PARAM_TARGET_REGIONS = 'target_regions'
PARAM_DETAILED = 'detailed'
PARAM_GET_URL = 'get_url'
PARAM_ACTIVATION_DATE = 'activation_date'
PARAM_REGIONS = 'regions'
PARAM_REGION = 'region'
PARAM_GIT_ACCESS_SECRET = 'git_access_secret'
PARAM_GIT_ACCESS_TYPE = 'git_access_type'
PARAM_GIT_PROJECT_ID = 'git_project_id'
PARAM_GIT_REF = 'git_ref'
PARAM_GIT_RULES_PREFIX = 'git_rules_prefix'
PARAM_GIT_URL = 'git_url'
PARAM_LAST_SYNC_CURRENT_STATUS = 'latest_sync_current_status'
PARAM_RULE_ID = 'rule_id'
PARAM_CUSTOMER_DISPLAY_NAME = 'customer_display_name'
PARAM_TENANT_DISPLAY_NAME = 'tenant_display_name'
PARAM_TENANT_NAME = 'tenant_name'
PARAM_STATUS_CODE = 'status_code'
PARAM_STATUS_REASON = 'status_reason'
PARAM_DESCRIPTION = 'description'
PARAM_SERVICE_SECTION = 'service_section'
PARAM_UPDATED_DATE = 'updated_date'
PARAM_ACCOUNT_DISPLAY_NAME = 'account_display_name'
PARAM_JOB_OWNER = 'job_owner'
PARAM_SCAN_REGIONS = 'scan_regions'
PARAM_SCAN_RULESETS = 'scan_rulesets'
PARAM_STATUS = 'status'
PARAM_SUBMITTED_AT = 'submitted_at'
PARAM_CREATED_AT = 'created_at'
PARAM_STARTED_AT = 'started_at'
PARAM_STOPPED_AT = 'stopped_at'
PARAM_SUCCESSFUL_CHECKS = 'successful_checks'
PARAM_FAILED_CHECKS = 'failed_checks'
PARAM_TOTAL_CHECKS = 'total_checks_performed'
PARAM_TOTAL_RESOURCES_VIOLATED_RULES = 'total_resources_violated_rules'
PARAM_LIMIT = 'limit'
PARAM_OFFSET = 'offset'
PARAM_NEXT_TOKEN = 'next_token'
PARAM_ACTIVE = 'active'
PARAM_FULL_CLOUD = 'full_cloud'
PARAM_START = 'start'
PARAM_END = 'end'
PARAM_DIRECT_OUTPUT = 'direct_output'
PARAM_ACTION = 'action'
PARAM_EVENT_DRIVEN = 'event_driven'
PARAM_RULE_VERSION = 'rule_version'
PARAM_ALL = 'all'
PARAM_LICENSE_HASH_KEY = "license_key"
PARAM_LICENSE_KEYS = 'license_keys'
PARAM_LICENSE_KEYS_TO_PREPEND = 'license_keys_to_prepend'
PARAM_LICENSE_KEYS_TO_APPEND = 'license_keys_to_append'
PARAM_LICENSE_KEYS_TO_DETACH = 'license_keys_to_detach'
PARAM_TENANT_LICENSE_KEY = 'tenant_license_key'
PARAM_CUSTOMER_NAME = "customer_name"
PARAM_CUSTOMERS = 'customers'
PARAM_RULESET_IDS = 'ruleset_ids'
PARAM_LATEST_SYNC = 'latest_sync'
PARAM_ADD_CUSTOMERS = 'add_customers'
PARAM_REMOVE_CUSTOMERS = 'remove_customers'
PARAM_COMPLETE = 'complete'
PARAM_SEND_SCAN_RESULT = 'send_scan_result'
PARAM_RULESET_LICENSE_PRIORITY = 'ruleset_license_priority'
PARAM_TENANT_DISPLAY_NAMES = 'tenant_display_names'
PARAM_TENANT_NAMES = 'tenant_names'

PARAM_GOVERNANCE_ENTITY_TYPE = 'governance_entity_type'
PARAM_GOVERNANCE_ENTITY_ID = 'governance_entity_id'
PARAM_MANAGEMENT_ID = 'management_id'
PARAM_RULESET = 'ruleset'

ACTION_COMPLIANCE_REPORT = 'compliance_report'
ACTION_RULE_REPORT = 'rule_report'
ACTION_ERROR_REPORT = 'error_report'
ACTION_PUSH_REPORT = 'push_report_to_siem'

PARAM_USERNAME = 'username'
PARAM_PASSWORD = 'password'
PARAM_ROLE = 'role'

PARAM_DOJO = 'dojo'
PARAM_DOJO_HOST = 'host'
PARAM_DOJO_APIKEY = 'api_key'
PARAM_DOJO_USER = 'user'
PARAM_DOJO_DISPLAY_ALL_FIELDS = 'display_all_fields'
PARAM_DOJO_UPLOAD_FILES = 'upload_files'
PARAM_DOJO_RESOURCE_PER_FINDING = 'resource_per_finding'
PARAM_DOJO_SEVERITY = 'severity'
PARAM_DOJO_LIMIT = 'limit'
PARAM_DOJO_OFFSET = 'offset'

PARAM_LM_HOST = 'host'
PARAM_LM_PORT = 'port'
PARAM_LM_VERSION = 'version'

PARAM_KEY_ID = 'key_id'
PARAM_PRIVATE_KEY = 'private_key'
PARAM_ALGORITHM = 'algorithm'
PARAM_FORMAT = 'format'
PARAM_B64ENCODED = 'b64_encoded'

PARAM_CHECK_PERMISSION = 'check_permission'

API_CUSTOMER = 'customers'
API_APPLICATION = 'applications'
API_ACCESS_APPLICATION = 'applications/access'
API_DOJO_APPLICATION = 'applications/dojo'
API_PARENT = 'parents'
API_PARENT_TENANT_LINK = 'parents/tenant-link'
API_CUSTOMER_RULESET = 'customers/ruleset'
API_RULESET = 'rulesets'
API_ED_RULESET = 'rulesets/event-driven'
API_RULE_SOURCE = 'rule-sources'
API_CUSTOMER_RULE_SOURCE = 'customers/rule/source'
API_TENANT = 'tenants'
API_TENANT_REGIONS = 'tenants/regions'
API_TENANT_LICENSE_PRIORITIES = 'tenants/license-priorities'
API_TENANT_RULE_SOURCE = 'tenants/rule/source'
API_ACCOUNT = 'accounts'
API_CREDENTIALS_MANAGER = 'accounts/credential_manager'
API_ACCOUNT_RULESET = 'accounts/ruleset'
API_ACCOUNT_RULE_SOURCE = 'accounts/rule/source'
API_ACCOUNT_REGION = 'accounts/regions'
API_POLICY = 'policies'
API_POLICY_CACHE = 'policies/cache'
API_ROLE = 'roles'
API_ROLE_CACHE = 'roles/cache'
API_RULE = 'rules'
API_RULE_META_UPDATER = 'rules/update-meta'
API_BACKUPPER = 'backup'
API_METRICS_UPDATER = 'metrics/update'
API_METRICS_STATUS = 'metrics/status'
API_JOB = 'jobs'
API_SIGNIN = 'signin'
API_SIGNUP = 'signup'
API_LICENSE = 'license'
API_LICENSE_SYNC = "license/sync"
API_FINDINGS = 'findings'
API_SCHEDULED_JOBS = 'scheduled-job'
API_USERS = 'users'
API_USER_TENANTS = 'users/tenants'
API_MAIL_SETTING = 'settings/mail'
API_LM_CONFIG_SETTING = 'settings/license-manager/config'
API_LM_CLIENT_SETTING = 'settings/license-manager/client'
API_BATCH_RESULTS = 'batch_results'
EVENT_RESOURCE = 'event'

API_HEALTH_CHECK = 'health'
API_SIEM_DOJO = 'siem/defect-dojo'
API_SIEM_SECURITY_HUB = 'siem/security-hub'

API_REPORT = 'report'
API_EVENT_DRIVEN_REPORT = 'reports/event-driven'
API_PUSH_REPORTS = 'reports/push'
API_REPORTS_PUSH_DOJO = 'reports/push/dojo'
API_REPORTS_PUSH_SECURITY_HUB = 'reports/push/security-hub'
API_DIGESTS_REPORTS = 'reports/digests'
API_DETAILS_REPORTS = 'reports/details'
API_COMPLIANCE_REPORTS = 'reports/compliance'
API_ERROR_REPORTS = 'reports/errors'
API_RULES_REPORTS = 'reports/rules'

API_OPERATIONAL_REPORT = 'reports/operational'
API_DEPARTMENT_REPORT = 'reports/department'
API_PROJECT_REPORT = 'reports/project'
API_C_LEVEL_REPORT = 'reports/clevel'

API_RABBITMQ = 'customers/rabbitmq'


PARAM_ID = 'id'
PARAM_RULE_SOURCE_ID = 'rule_source_id'

MODEL_CUSTOMER = 'CUSTOMER'
MODEL_TENANT = 'TENANT'
MODEL_ACCOUNT = 'ACCOUNT'

POLICIES_TO_ATTACH = 'policies_to_attach'
POLICIES_TO_DETACH = 'policies_to_detach'

PERMISSIONS_TO_ATTACH = 'permissions_to_attach'
PERMISSIONS_TO_DETACH = 'permissions_to_detach'

RULES_TO_ATTACH = 'rules_to_attach'
RULES_TO_DETACH = 'rules_to_detach'

GIT_ACCESS_TOKEN = 'TOKEN'
AVAILABLE_GIT_ACCESS_TYPES = (GIT_ACCESS_TOKEN,)
ALL = 'ALL'
SPECIFIC_TENANT = 'SPECIFIC_TENANT'
AWS, AZURE, GCP, GOOGLE = 'AWS', 'AZURE', 'GCP', 'GOOGLE'
AVAILABLE_CLOUDS = (AWS, AZURE, GCP)
REGION_STATE_ACTIVE = 'ACTIVE'
REGION_STATE_INACTIVE = 'INACTIVE'
AVAILABLE_REGION_STATES = (REGION_STATE_ACTIVE, REGION_STATE_INACTIVE)

PARAM_CONFIGURATION_TYPE = 'configuration_type'
PARAM_CONFIGURATION = 'configuration'

PARAM_ENTITIES_MAPPING = 'entities_mapping'
PARAM_CLEAR_EXISTING_MAPPING = 'clear_existing_mapping'
PARAM_PRODUCT_TYPE_NAME = 'product_type_name'
PARAM_PRODUCT_NAME = 'product_name'
PARAM_ENGAGEMENT_NAME = 'engagement_name'
PARAM_TEST_TITLE = 'test_title'

PARAM_API_VERSION = 'api_version'

PARAM_TENANT_ALLOWANCE = 'tenant_allowance'
PARAM_TENANT_RESTRICTION = 'tenant_restriction'
PARAM_TENANT_ACCOUNT_EXCLUSION = 'tenant_account_exclusion'

HEALTH_CHECK_COMMAND_NAME = 'health_check'
CLEAN_CACHE_COMMAND_NAME = 'clean_cache'
UPDATE_RULES_COMMAND_NAME = 'update_rules'
RULE_SOURCE_GROUP_NAME = 'rule_source'
CREDENTIALS_MANAGER_GROUP_NAME = 'credentials_manager'
SECURITY_HUB_COMMAND_NAME = 'security_hub'

ITEMS_ATTR = 'items'
MESSAGE_ATTR = 'message'
TRACE_ID_ATTR = 'trace_id'
NEXT_TOKEN_ATTR = 'next_token'

PARAM_RAW = 'raw'
PARAM_EXPAND_ON = 'expand_on'
PARAM_REGIONS_TO_INCLUDE = 'regions_to_include'
PARAM_RESOURCE_TYPES_TO_INCLUDE = 'resource_types_to_include'
PARAM_SEVERITIES_TO_INCLUDE = 'severities_to_include'
PARAM_DATA_TYPE = 'data_type'
PARAM_MAP_KEY = 'map_key'
PARAM_DEPENDENT_INCLUSION = 'dependent_inclusion'
PARAM_SCHEDULE_EXPRESSION = 'schedule'
PARAM_TARGET_USER = 'target_user'

PARAM_DISCLOSE = 'disclose'
PARAM_PASSWORD_ALIAS = 'password_alias'
PARAM_HOST = 'host'
PARAM_PORT = 'port'
PARAM_MAX_EMAILS = 'max_emails'
PARAM_DEFAULT_SENDER = 'default_sender'
PARAM_USE_TLS = 'use_tls'

PARAM_EVENT_TYPE = 'event_type'
PARAM_EVENT_BODY = 'event_body'
PARAM_EVENTS = 'events'
PARAM_VENDOR = 'vendor'

C7NCLI_LOG_LEVEL_ENV_NAME = 'C7NCLI_LOG_LEVEL'
C7NCLI_DEVELOPER_MODE_ENV_NAME = 'C7N_CLI_DEVELOPER_MODE'

CLOUDTRAIL_EVENT_TYPE = 'CloudTrail'
EVENTBRIDGE_EVENT_TYPE = 'EventBridge'

PARAM_START_ISO = 'start_iso'
PARAM_END_ISO = 'end_iso'
PARAM_HREF = 'href'
PARAM_TYPE = 'type'
PARAM_JOBS = 'jobs'
PARAM_RULE = 'rule'
MANUAL_JOB_TYPE = 'manual'
REACTIVE_JOB_TYPE = 'reactive'
AVAILABLE_JOB_TYPES = (
    MANUAL_JOB_TYPE, REACTIVE_JOB_TYPE
)

# Credentials
ENV_AWS_ACCESS_KEY_ID = 'AWS_ACCESS_KEY_ID'
ENV_AWS_SECRET_ACCESS_KEY = 'AWS_SECRET_ACCESS_KEY'
ENV_AWS_SESSION_TOKEN = 'AWS_SESSION_TOKEN'
ENV_AWS_DEFAULT_REGION = 'AWS_DEFAULT_REGION'
ENV_AWS_REGION = 'AWS_REGION'

ENV_AZURE_TENANT_ID = 'AZURE_TENANT_ID'
ENV_AZURE_SUBSCRIPTION_ID = 'AZURE_SUBSCRIPTION_ID'
ENV_AZURE_CLIENT_ID = 'AZURE_CLIENT_ID'
ENV_AZURE_CLIENT_SECRET = 'AZURE_CLIENT_SECRET'

ENV_GOOGLE_APPLICATION_CREDENTIALS = 'GOOGLE_APPLICATION_CREDENTIALS'

DEFAULT_AWS_REGION = 'us-east-1'

PARAM_AWS_ACCESS_KEY = ENV_AWS_ACCESS_KEY_ID
PARAM_AWS_SECRET_ACCESS_KEY = ENV_AWS_SECRET_ACCESS_KEY
PARAM_AWS_DEFAULT_REGION = ENV_AWS_DEFAULT_REGION
PARAM_AWS_SESSION_TOKEN = ENV_AWS_SESSION_TOKEN

PARAM_AZURE_TENANT_ID = ENV_AZURE_TENANT_ID
PARAM_AZURE_SUBSCRIPTION_ID = ENV_AZURE_SUBSCRIPTION_ID
PARAM_AZURE_CLIENT_ID = ENV_AZURE_CLIENT_ID
PARAM_AZURE_CLIENT_SECRET = ENV_AZURE_CLIENT_SECRET


# responses
ADAPTER_NOT_CONFIGURED_MESSAGE = \
    'Custodian Service API link is not configured. ' \
    'Run \'c7n configure\' and try again.'
MALFORMED_RESPONSE_MESSAGE = \
    'Malformed response obtained. Please contact ' \
    'support team for assistance.'
NO_ITEMS_TO_DISPLAY_RESPONSE_MESSAGE = 'No items to display'
NO_CONTENT_RESPONSE_MESSAGE = 'Request is successful. No content returned'  # 204

# codes
RESPONSE_NO_CONTENT = 204

CONFIG_FOLDER = '.c7n'

CONTEXT_API_CLIENT = 'api_client'
CONTEXT_CONFIG = 'config'
CONTEXT_MODULAR_ADMIN_USERNAME = 'modular_admin_username'


CONF_ACCESS_TOKEN = 'access_token'
CONF_API_LINK = 'api_link'
CONF_ITEMS_PER_COLUMN = 'items_per_column'

MODULE_NAME = 'c7n'  # for modular admin
